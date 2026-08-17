# BITÁCORA TÉCNICA — market-data-medallion

> Documento de estudio, no de continuidad para agentes de IA (ese es `BITACORA_MAESTRA.md` del repo del sitio). Este archivo existe para que Davirson entienda, línea por línea, cómo funciona el pipeline que construimos: cada conexión de API, cada query SQL, cada test, y por qué. Cada bloque técnico va seguido de un `» NEGOCIO:` que conecta esa pieza con el problema real que resuelve.
>
> Nivel asumido: sabes Python, SQL, Power BI y conectarte a APIs a nivel básico. No te explico qué es una función o un JOIN — te explico las decisiones de diseño y por qué el código quedó exactamente así.

---

## 0. Mapa del repo

```
market-data-medallion/
├── config.yaml                    ← qué activos, qué fuentes, qué estrategias, qué comisiones
├── db/migrations/001_init.sql     ← DDL: crea los esquemas y tablas en Postgres
├── pipeline/
│   ├── config.py                  ← carga config.yaml + variables de entorno (.env)
│   ├── models.py                  ← las clases Candle e IngestResult (contratos de datos)
│   ├── sources/
│   │   ├── base.py                ← HTTP compartido: reintentos, protocolo común
│   │   ├── coinbase.py            ← cliente Coinbase
│   │   ├── kraken.py              ← cliente Kraken
│   │   └── tiingo.py              ← cliente Tiingo
│   ├── bronze.py                  ← orquesta: pide watermark, llama al cliente, inserta
│   ├── backtest/
│   │   ├── strategies.py          ← las 4 estrategias
│   │   ├── engine.py              ← el simulador (ejecución, fees, slippage)
│   │   └── metrics.py             ← retorno, CAGR, drawdown, Sharpe
│   ├── quality.py                 ← el "portero" pandera antes de backtestear
│   ├── export.py                  ← arma el JSON final para el sitio
│   └── flows.py                   ← el orquestador Prefect que amarra todo
├── dbt/
│   ├── models/staging/             ← capa silver (limpieza)
│   ├── models/marts/               ← capa gold (indicadores + reportes)
│   └── tests/                      ← 33 pruebas de calidad de datos
├── tests/                          ← 77 pruebas unitarias de Python
└── .github/workflows/              ← automatización en la nube
```

**» NEGOCIO:** esta estructura es literalmente el organigrama de un equipo de datos: quién trae la materia prima (`sources/`), quién la procesa (`bronze.py` + `dbt/staging`), quién calcula los indicadores de gestión (`dbt/marts`), quién corre las simulaciones (`backtest/`), y quién arma el reporte final (`export.py`). Cada carpeta es un "puesto de trabajo" con una responsabilidad y nadie más la toca.

---

## 1. La conexión a las APIs — código real, tres formas distintas de la misma idea

### 1.1 El problema que resuelve `sources/base.py`

Antes de mirar cada cliente, mira lo que comparten. Tres proveedores distintos, tres formatos de respuesta distintos, pero **una sola forma de pedir HTTP con reintentos**:

```python
# pipeline/sources/base.py
def request_json(session, url, *, params=None, headers=None,
                  timeout=30.0, max_tries=3, backoff_seconds=None):
    last_status = 0
    for attempt in range(1, max_tries + 1):
        response = session.get(url, params=params, headers=headers, timeout=timeout)
        last_status = response.status_code
        if last_status == 429 or last_status >= 500:
            if attempt < max_tries:
                delay = BACKOFF_SECONDS if backoff_seconds is None else backoff_seconds
                time.sleep(delay * 2 ** (attempt - 1))
            continue
        response.raise_for_status()
        return response.json()
    raise SourceError(f"GET {url} failed with HTTP {last_status} after {max_tries} tries")
```

**Qué hace exactamente:** intenta el GET hasta 3 veces. Si la respuesta es `429` (Too Many Requests, te excediste del límite) o `5xx` (error del lado del servidor), espera y reintenta con **backoff exponencial**: 1 segundo, luego 2, luego 4 (`delay * 2 ** (attempt-1)`). Si es cualquier otro error (ej. `404`), `raise_for_status()` lo revienta inmediatamente — no tiene sentido reintentar un error que no se va a arreglar solo. Si agota los 3 intentos, lanza `SourceError`.

**» NEGOCIO:** las APIs gratuitas caen o se saturan momentáneamente — es normal, no es un bug tuyo. Sin este reintento, un proceso automático que corre de madrugada (cuando nadie está mirando) fallaría por un hipo de red de 2 segundos y tu tablero se quedaría sin actualizar ese día, sin que nadie se entere hasta que alguien pregunte "¿por qué el dato de ayer no está?". Este patrón — reintento con backoff exponencial — es estándar en cualquier sistema productivo serio; es la diferencia entre un script de estudiante y algo que corre solo en producción.

También define un **Protocol** de Python (una interfaz, como en POO):

```python
class SourceClient(Protocol):
    def fetch_candles(self, symbol: str, start: datetime, end: datetime) -> list[Candle]: ...
```

**» NEGOCIO:** esto es lo que permite que `bronze.py` (el orquestador) no necesite saber si está hablando con Coinbase, Kraken o Tiingo — solo sabe que cualquier "cliente" le va a dar una lista de velas cuando le pida un rango de fechas. Si mañana agregas Binance.US como cuarta fuente, escribes un archivo nuevo que cumpla este contrato y el resto del sistema ni se entera del cambio. Es el mismo principio de tener un formato estándar de solicitud de compra: no importa qué proveedor sea, todos llenan el mismo formulario.

### 1.2 Coinbase — paginación por ventanas de 300

```python
# pipeline/sources/coinbase.py
API_URL = "https://api.exchange.coinbase.com/products/{product}/candles"
GRANULARITY_SECONDS = 86_400          # 1 día
MAX_CANDLES_PER_REQUEST = 300
_FIELDS = ("time", "low", "high", "open", "close", "volume")

def fetch_candles(self, symbol, start, end):
    step = timedelta(seconds=GRANULARITY_SECONDS)
    window = step * (MAX_CANDLES_PER_REQUEST - 1)
    by_ts = {}
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + window, end)
        rows = request_json(self._session, API_URL.format(product=symbol),
            params={"granularity": GRANULARITY_SECONDS,
                    "start": cursor.isoformat(), "end": chunk_end.isoformat()},
            headers={"User-Agent": USER_AGENT})
        for row in rows:
            raw = dict(zip(_FIELDS, row, strict=True))
            ts = datetime.fromtimestamp(int(raw["time"]), tz=UTC)
            if start <= ts <= end:
                by_ts[ts] = Candle(source="coinbase", symbol=symbol, granularity="1d",
                    ts=ts, open=float(raw["open"]), high=float(raw["high"]),
                    low=float(raw["low"]), close=float(raw["close"]),
                    volume=float(raw["volume"]), raw=raw)
        cursor = chunk_end + step
    return [by_ts[ts] for ts in sorted(by_ts)]
```

**Qué hace exactamente:** la API de Coinbase solo te devuelve 300 velas por llamada. Si pides 4 años de historia diaria (~1,700 días), necesitas ~6 llamadas. El `while cursor <= end` avanza la ventana de 300 en 300 hasta cubrir todo el rango. Nota clave: **Coinbase devuelve arrays sin nombre** — `[time, low, high, open, close, volume]`, en ese orden fijo — así que `dict(zip(_FIELDS, row, strict=True))` le pone nombre a cada posición. El `strict=True` es una salvaguarda: si Coinbase algún día cambiara el número de campos que devuelve, esto lanzaría un error inmediato en vez de asignar mal los nombres silenciosamente.

**» NEGOCIO:** este límite de 300 por llamada es la razón de negocio detrás de escribir un cliente propio en vez de usar una librería genérica — necesitábamos control fino sobre la paginación para traer 4+ años de historia sin exceder límites de tasa. El `strict=True` es una decisión de "falla ruidosa, no silenciosa": preferimos que el proceso se caiga con un error claro a que guarde datos con las columnas cambiadas sin que nadie lo note por meses (ver el error real que tuvimos con Kraken, sección 6).

### 1.3 Kraken — la ventana fija de ~720 velas

```python
# pipeline/sources/kraken.py
PAIR_BY_SYMBOL = {"BTC-USD": "XBTUSD", "ETH-USD": "ETHUSD"}
_FIELDS = ("time", "open", "high", "low", "close", "vwap", "volume", "count")

def fetch_candles(self, symbol, start, end):
    pair = PAIR_BY_SYMBOL.get(symbol)
    payload = request_json(self._session, API_URL,
        params={"pair": pair, "interval": 1440}, headers={"User-Agent": USER_AGENT})
    if payload.get("error"):
        raise SourceError(f"Kraken error for {pair}: {payload['error']}")
    result = payload.get("result") or {}
    rows = next((v for k, v in result.items() if k != "last"), None)
    candles = []
    for row in rows:
        raw = dict(zip(_FIELDS, row, strict=True))
        ts = datetime.fromtimestamp(int(raw["time"]), tz=UTC)
        if start <= ts <= end:
            candles.append(Candle(source="kraken", symbol=symbol, ..., raw=raw))
    candles.sort(key=lambda c: c.ts)
    return candles
```

**Qué hace exactamente:** dos cosas curiosas de la API de Kraken que el código tiene que absorber:
1. **Mapeo de símbolos:** Kraken no usa "BTC-USD", usa "XBTUSD" (su propio código para Bitcoin) — de ahí el diccionario `PAIR_BY_SYMBOL`.
2. **Clave de respuesta impredecible:** Kraken responde `{"result": {"XXBTZUSD": [...velas...], "last": 123456}}` — el nombre de la clave con las velas (`XXBTZUSD`) NO es el mismo string que pediste (`XBTUSD`) y trae un campo extra `"last"` que no es una vela. `next((v for k, v in result.items() if k != "last"), None)` es la forma de decir "dame el primer valor cuyo key no sea 'last', sin importar cómo se llame exactamente".
3. **No pagina:** Kraken siempre devuelve sus ~720 velas más recientes sin importar qué rango pidas — por eso el filtro `if start <= ts <= end` recorta después de recibir, en vez de controlar la ventana antes de pedir (como sí hace Coinbase).

**» NEGOCIO:** Kraken solo te da los últimos ~2 años de historia diaria por esta vía gratuita — es una limitación real del proveedor, no nuestra. Por eso Kraken es la fuente de **reconciliación** (comparar contra Coinbase) y no la fuente primaria: no tiene suficiente historia para ser la fuente principal de 4+ años, pero sí la suficiente para servir de "segunda opinión" reciente. Es como usar un segundo banco solo para conciliar los últimos 2 años de movimientos, aunque tu contabilidad completa vaya 4 años atrás.

### 1.4 Tiingo — la única con autenticación

```python
# pipeline/sources/tiingo.py
def __init__(self, session=None, api_key=None):
    self._api_key = api_key if api_key is not None else tiingo_api_key()
    if not self._api_key:
        raise MissingApiKeyError("TIINGO_API_KEY is not set; configure it to ingest equities")

def fetch_candles(self, symbol, start, end):
    rows = request_json(self._session, API_URL.format(ticker=symbol),
        params={"startDate": start.date().isoformat(),
                "endDate": end.date().isoformat(), "token": self._api_key},
        headers={"User-Agent": USER_AGENT})
    ...
```

**Qué hace exactamente:** a diferencia de Coinbase/Kraken (públicas, sin credenciales), Tiingo exige un `token` como parámetro de la URL — la key que generaste anoche. Si no existe, lanza `MissingApiKeyError` **antes de intentar la llamada** — nunca gastamos una petición HTTP sabiendo de antemano que va a fallar.

**» NEGOCIO:** este es el patrón de **degradación controlada**: si mañana tu key de Tiingo expira o la borras, el sistema no se cae completo — `flows.py` atrapa específicamente este error y se salta SPY/QQQ con una advertencia, pero sigue ingiriendo BTC-USD y ETH-USD sin problema (lo viste anoche en los logs: `WARNING - TIINGO_API_KEY not set — skipping tiingo for SPY`). Un fallo parcial controlado es infinitamente mejor que un fallo total.

---

## 2. `bronze.py` — el orquestador de ingesta y el concepto de "watermark"

Esta es la pieza más importante para entender cómo el sistema sabe **qué día es "nuevo"** sin volver a pedir 4 años de historia cada vez que corre.

```python
# pipeline/bronze.py
_WATERMARK_SQL = """
SELECT max(candle_ts) FROM bronze.raw_candles
WHERE source = %s AND symbol = %s AND granularity = %s
"""

def ingest(conn, source_name, asset, granularity):
    ...
    watermark = _watermark(conn, source_name, asset.symbol, granularity)
    window_start = (
        watermark + timedelta(days=1) if watermark is not None
        else _as_utc(asset.backfill_start)
    )
    window_end = _last_closed_day(started_at)
    if window_start <= window_end:
        candles = client.fetch_candles(asset.symbol, window_start, window_end)
```

**Qué hace exactamente:** un **watermark** ("marca de agua") es, en términos simples, "hasta dónde ya llegué la última vez". La query busca la fecha más reciente que ya tenemos guardada para esa combinación exacta de (fuente, símbolo, granularidad). Si existe, empezamos al día siguiente (`+ timedelta(days=1)`) — no repetimos trabajo. Si no existe (primera vez), arrancamos desde `backfill_start` (2022-01-01 en tu `config.yaml`).

`_last_closed_day` calcula el final de la ventana:
```python
def _last_closed_day(now):
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
```
Esto trunca la hora actual a medianoche y le resta un día — es decir, **el proceso nunca pide la vela de hoy**, porque hoy todavía no cerró (una vela diaria solo es "final" cuando el día terminó). Si pidiéramos la vela de hoy a mitad de camino, la guardaríamos incompleta y para siempre — recuerda que bronze es append-only, nunca se actualiza un registro ya escrito.

**» NEGOCIO:** este mecanismo es exactamente lo que hace que el proceso sea **incremental**: corre en 2 segundos en el día a día (solo trae 1 vela nueva por fuente) en vez de re-descargar 4 años cada vez, como haría un script mal diseñado. Es la diferencia entre "actualizar el estado de cuenta agregando solo los movimientos del día" versus "re-imprimir el estado de cuenta completo de los últimos 4 años cada mañana". Además, nunca captura un dato "a medio cocinar" — la misma disciplina que un contador aplica al no cerrar un asiento hasta que el día contable terminó.

### 2.1 Idempotencia: por qué correr esto dos veces no duplica nada

```python
_INSERT_CANDLE_SQL = """
INSERT INTO bronze.raw_candles (source, symbol, granularity, candle_ts, payload, ingest_run_id)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (source, symbol, granularity, candle_ts) DO NOTHING
"""
```

**Qué hace exactamente:** la tabla tiene una restricción `UNIQUE (source, symbol, granularity, candle_ts)` (la ves en `db/migrations/001_init.sql`). Si intentas insertar una fila que ya existe con esa combinación exacta, Postgres normalmente lanzaría un error de violación de restricción — pero `ON CONFLICT ... DO NOTHING` le dice "si ya existe, simplemente ignora esta fila e insertar las demás sin fallar".

**» NEGOCIO:** esto es lo que se llama **idempotencia** — la propiedad de que ejecutar la misma operación una vez o cien veces produce el mismo resultado final. Es crítico para automatización real: si el cron de GitHub Actions corre dos veces por un reintento de la plataforma, o si tú corres el proceso manualmente para probar algo, **nunca vas a duplicar datos ni a corromper el historial**. Sin esto, cada re-ejecución accidental sería un riesgo de inflar tus números.

### 2.2 La auditoría — cada intento queda registrado, gane o pierda

```python
try:
    with conn.transaction():
        conn.execute(_INSERT_RUN_SQL, (run_id, source_name, ..., status, error, started_at, None))
        for candle in candles:
            cursor = conn.execute(_INSERT_CANDLE_SQL, (...))
            rows_inserted += max(cursor.rowcount, 0)
        conn.execute(_FINISH_RUN_SQL, (rows_inserted, datetime.now(UTC), run_id))
except Exception as exc:
    status = "failed"
    _record_failed_run(conn, run_id, source_name, ...)
```

**Qué hace exactamente:** todo el bloque de inserción va dentro de `conn.transaction()` — una **transacción** significa que TODAS las operaciones dentro tienen éxito juntas, o NINGUNA se aplica (se revierte todo, "rollback"). Si algo falla a mitad de la inserción de 400 velas, no te quedas con 230 velas insertadas y 170 perdidas en el limbo — o entran las 400, o no entra ninguna, y el error queda registrado en `meta.ingest_runs`.

**» NEGOCIO:** esta es la tabla de auditoría de la que te hablé — literalmente el "log de auditoría interna" de cada corrida del pipeline: qué fuente, qué símbolo, cuántas filas trajo, cuántas insertó realmente (pueden diferir por el `ON CONFLICT DO NOTHING`), si tuvo éxito o falló y por qué. Fue justamente esta tabla la que nos permitió detectar el "Error 2" que viste anoche (el watermark envenenado) — vimos 3 corridas con timestamp idéntico exacto `00:00:00`, algo que un proceso real jamás produce, y eso encendió la alarma.

---

## 3. La base de datos — el DDL completo, explicado

```sql
-- db/migrations/001_init.sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS bronze.raw_candles (
    raw_candle_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source          TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    granularity     TEXT NOT NULL DEFAULT '1d',
    candle_ts       TIMESTAMPTZ NOT NULL,
    payload         JSONB NOT NULL,
    ingest_run_id   UUID NOT NULL REFERENCES meta.ingest_runs (ingest_run_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, symbol, granularity, candle_ts)
);
```

**Términos clave, uno por uno:**

- **`SCHEMA`**: es literalmente una "carpeta" dentro de una misma base de datos. `bronze`, `silver`, `gold`, `meta` son 4 carpetas dentro de la misma base `mdm` — no son 4 bases distintas. Te deja tener `bronze.raw_candles` y, en teoría, otra tabla llamada `raw_candles` en otro esquema, sin que choquen.
- **`GENERATED ALWAYS AS IDENTITY`**: es el equivalente moderno de Postgres a un "autonumérico" — Postgres asigna el número de fila automáticamente, tú nunca lo especificas.
- **`TIMESTAMPTZ`** (timestamp with time zone): guarda el instante exacto en UTC internamente, sin importar en qué zona horaria lo insertaste — Postgres hace la conversión de entrada/salida según el `TimeZone` de la sesión que está consultando (esto es EXACTAMENTE lo que causó el Error 1 que viste anoche — ver sección 6.2).
- **`JSONB`**: un tipo de columna que guarda JSON en formato binario indexable (más rápido de consultar que texto plano JSON). Aquí guardamos el payload crudo de la API tal cual llegó — es tu "papel soporte digital".
- **`REFERENCES meta.ingest_runs (ingest_run_id)`**: esto es una **llave foránea (foreign key)** — Postgres no te deja insertar una vela con un `ingest_run_id` que no exista en la tabla de auditoría. Es una regla de integridad referencial a nivel de base de datos, no solo una convención de código: aunque el programa Python tuviera un bug, la base de datos misma rechazaría el dato huérfano.
- **`UNIQUE (source, symbol, granularity, candle_ts)`**: la restricción que hace posible el `ON CONFLICT DO NOTHING` de la sección anterior.

**» NEGOCIO:** estas reglas ("constraints") son la diferencia entre confiar en que el código Python nunca tenga un bug (ingenuo) y que la propia base de datos rechace físicamente datos inconsistentes sin importar qué programa intente escribirlos (robusto). Es el equivalente a que un sistema contable no te deje contabilizar un asiento sin una cuenta contable válida, sin importar quién lo esté digitando.

```sql
CREATE TABLE IF NOT EXISTS gold.backtest_runs (
    backtest_run_id UUID PRIMARY KEY,
    ...
    total_return    NUMERIC,
    max_drawdown    NUMERIC,
    sharpe          NUMERIC,
    ...
);
CREATE TABLE IF NOT EXISTS gold.backtest_equity_curves (
    backtest_run_id UUID NOT NULL REFERENCES gold.backtest_runs (backtest_run_id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    equity          NUMERIC NOT NULL,
    buy_hold_equity NUMERIC NOT NULL,
    PRIMARY KEY (backtest_run_id, ts)
);
```

**Qué hace exactamente:** `gold.backtest_runs` guarda un resumen (una fila) por cada backtest corrido — símbolo, estrategia, parámetros usados, y todas las métricas finales. `gold.backtest_equity_curves` guarda el detalle día a día (una fila por cada punto de la curva de resultados) y apunta de vuelta a su resumen con `ON DELETE CASCADE`: si borras el resumen, Postgres borra automáticamente todos sus puntos de curva asociados, sin dejar basura huérfana.

**» NEGOCIO:** esta separación resumen/detalle es el mismo patrón que un estado financiero (el resumen: utilidad neta del año) contra el mayor contable (el detalle: cada transacción que sumó a esa utilidad). El resumen te sirve para comparar rápido 16 backtests entre sí; el detalle te sirve para dibujar el gráfico de la curva de equity de uno en particular.

---

## 4. dbt — las 33 pruebas de calidad de datos, una por una

**Qué es dbt en una frase técnica:** una herramienta que te deja escribir transformaciones SQL como archivos `.sql` versionados en Git, con dependencias explícitas entre ellos (`{{ ref('otro_modelo') }}` = "este modelo depende de ese otro"), y pruebas automáticas de calidad que corren después de construir cada tabla.

### 4.1 Capa silver — de crudo a limpio

**`stg_ohlcv_all_sources.sql`** — parsea el JSON crudo a columnas tipadas:

```sql
select
    source, symbol, granularity, candle_ts as ts,
    (payload ->> 'open')::numeric   as open,
    (payload ->> 'high')::numeric   as high,
    (payload ->> 'low')::numeric    as low,
    (payload ->> 'close')::numeric  as close,
    (payload ->> 'volume')::numeric as volume,
    coalesce(volume = 0, false)                                     as is_zero_volume,
    (open is null or high is null or low is null or close is null)  as has_null_price,
    coalesce(high < low, false)                                     as high_lt_low
from {{ source('bronze', 'raw_candles') }}
```

**Qué hace exactamente:** `payload ->> 'open'` es sintaxis de Postgres para "sácame el valor de la clave `open` del JSON, como texto"; `::numeric` lo convierte a número. Como el `payload` de las tres fuentes ya llega **con las mismas claves nombradas** (gracias a los `_FIELDS` de cada cliente en la sección 1), esta única query sirve para las tres fuentes sin necesidad de un `CASE WHEN source = ...` distinto por cada una (esto NO era así originalmente — ver Error 3 en la sección 6.3, fue justo el bug que arreglamos).

Además calcula 3 **banderas de calidad** (`is_zero_volume`, `has_null_price`, `high_lt_low`) — no filtra ni borra filas sospechosas, las **marca**. Esa es una decisión de diseño deliberada.

**» NEGOCIO:** marcar en vez de borrar es el mismo principio que una conciliación bancaria: no eliminas la transacción rara, la señalas como "pendiente de revisión" y sigue existiendo para que alguien decida. Si borráramos silenciosamente cualquier vela con volumen cero, perderíamos evidencia de un problema real de la fuente (o de un día festivo legítimo con muy poco volumen) sin dejar rastro.

**`stg_ohlcv.sql`** — deduplica a una sola fila por (símbolo, día):

```sql
with ranked as (
    select *,
        row_number() over (
            partition by symbol, granularity, ts
            order by case source when 'coinbase' then 1 when 'kraken' then 2
                                  when 'tiingo' then 3 else 4 end, source
        ) as source_rank
    from {{ ref('stg_ohlcv_all_sources') }}
)
select ... from ranked where source_rank = 1
```

**Qué hace exactamente:** `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` es una **función de ventana (window function)** — numera las filas dentro de cada grupo (`PARTITION BY symbol, granularity, ts` = "agrupa todas las filas que sean el mismo símbolo, el mismo día") según el orden que le des. Aquí el orden es un `CASE` que le asigna prioridad 1 a Coinbase, 2 a Kraken, 3 a Tiingo — así que dentro de cada grupo, la fila de Coinbase siempre queda numerada como `1`. El `WHERE source_rank = 1` se queda solo con esa.

**» NEGOCIO:** esto resuelve la pregunta "si tengo el mismo día de BTC-USD en Coinbase Y en Kraken, ¿cuál es 'el' precio oficial de ese día en mis reportes?" — la respuesta que codificamos es "Coinbase manda, Kraken es respaldo/conciliación". Es una decisión de negocio (cuál fuente es la autoridad) traducida a una línea de SQL, documentada y versionada — no una elección arbitraria oculta en la cabeza de alguien.

### 4.2 Capa gold — los indicadores técnicos, calculados en SQL puro

Ya viste el código de `fct_ohlcv_indicators.sql` arriba. Los puntos que vale la pena resaltar como decisiones deliberadas:

```sql
avg(close) over w20 as avg_close_20,
...
window w20 as (partition by symbol order by ts rows between 19 preceding and current row)
```

**Por qué `ROWS BETWEEN 19 PRECEDING AND CURRENT ROW` y no simplemente "los últimos 20 días":** en SQL, una ventana puede definirse por `ROWS` (cuenta física de filas) o por `RANGE` (por valor, ej. por fecha). Sin especificarlo explícitamente, Postgres puede usar un `RANGE` implícito que en ciertos casos incluye MÁS filas de las que crees si hay fechas duplicadas — la ventana `ROWS` es inequívoca: exactamente 20 filas (la actual + 19 anteriores), sin ambigüedad.

```sql
case when rn >= 20 then avg_close_20 end as sma_20,
```

**Por qué el `CASE WHEN rn >= 20`:** si no lo hicieras, en el día 5 de historia, `avg(close) over w20` te daría el promedio de esos 5 días disponibles — un "promedio de 20 días" que en realidad es un promedio de 5, silenciosamente. El `CASE` fuerza `NULL` explícito hasta que existan las 20 filas completas.

**» NEGOCIO:** esto es matemáticamente crítico y es exactamente el tipo de error que un analista sin experiencia comete en Excel al arrastrar una fórmula de promedio móvil sin fijar el rango — obtienes un número que *parece* válido pero está calculado sobre menos datos de los que el nombre de la columna promete. Un SMA-20 basado en 5 días no es un SMA-20, es un número con nombre falso. Preferimos un `NULL` honesto ("todavía no tengo suficiente historia") a un número silenciosamente incorrecto.

```sql
case when rn >= 15
    then 100.0 * avg_gain_14 / nullif(avg_gain_14 + avg_loss_14, 0)
end as rsi_14,
```

**El RSI (Índice de Fuerza Relativa) explicado en su fórmula real:** el RSI clásico se define como `100 - 100/(1+RS)` donde `RS = avg_gain/avg_loss`. Nuestra fórmula, `100 * avg_gain/(avg_gain+avg_loss)`, es **algebraicamente idéntica** (puedes despejarlo tú mismo con papel y lápiz) pero evita una división por cero cuando `avg_loss = 0` (mercado solo subiendo, ningún día rojo en la ventana) — con la fórmula clásica eso te daría `RS = infinito` y tocaría un caso especial aparte; con la nuestra, simplemente da 100 directamente. `NULLIF(x, 0)` es la función de Postgres para "si esto es cero, dame NULL en vez de dividir por cero".

`rn >= 15` porque el RSI de 14 periodos necesita 14 *diferencias* día a día, y una diferencia necesita 2 precios — el primer día de historia no tiene "día anterior", así que necesitas 15 filas para tener 14 diferencias completas.

```sql
stddev_pop(close) over w20 as stddev_close_20,
...
avg_close_20 + 2 * stddev_close_20 as bb_upper_20,
```

**Bandas de Bollinger:** banda superior = media móvil + 2 desviaciones estándar; banda inferior = media - 2 desviaciones. Nota: usamos `stddev_pop` (desviación estándar **poblacional**, divide entre N) y no `stddev_samp` (desviación **muestral**, divide entre N-1) — esto lo corregimos anoche en la revisión adversarial. La definición canónica de Bollinger (la que inventó John Bollinger, la que usa TradingView/TA-Lib) usa la poblacional; con la muestral, la banda queda ~2.6% más ancha de lo estándar, que no es "incorrecto" matemáticamente pero sí distinto del indicador que todo el mundo reconoce con ese nombre.

**» NEGOCIO:** este archivo entero es la prueba de dominio SQL más fuerte de todo el proyecto para un reclutador — no llamamos a una librería de indicadores técnicos (como TA-Lib), los derivamos nosotros desde la definición matemática usando funciones de ventana puras. Cualquier data engineer que lea este archivo entiende inmediatamente que sabes manejar `PARTITION BY`, framing explícito de ventanas, y casos borde de división por cero — eso es SQL de nivel de producción, no de tutorial.

### 4.3 Los marts de negocio

**`mart_source_reconciliation.sql`** — ya la viste, es el JOIN de Coinbase vs Kraken por (symbol, ts) calculando `abs_pct_diff` y marcando `is_discrepant` si supera 0.5%.

**» NEGOCIO:** este es tu control de calidad "conciliación bancaria" hecho SQL. El resultado real de anoche: 0.16% de diferencia máxima entre las dos fuentes, 0 días discrepantes. Eso es evidencia objetiva, no una promesa, de que tus datos crudos son confiables.

**`mart_data_quality.sql`** — usa `generate_series` para construir el calendario esperado:

```sql
cross join lateral generate_series(p.first_date, p.last_date, interval '1 day') as d (day)
where p.asset_class = 'crypto' or extract(isodow from d.day) <= 5
```

**Qué hace exactamente:** `generate_series` genera una fila por cada día entre la primera y última fecha que tienes — literalmente construye un calendario. `extract(isodow from d.day) <= 5` filtra a solo lunes-viernes (`isodow` numera 1=lunes...7=domingo) — así el "número de días esperados" para una acción/ETF excluye automáticamente los fines de semana (el mercado no opera), mientras que para cripto cuenta los 365/366 días del año porque cripto opera 24/7/365.

**» NEGOCIO:** esta es la razón exacta por la que anoche SPY y QQQ mostraron "47 días faltantes" y tú no debes preocuparte por eso — son los festivos bursátiles de EE.UU. (Acción de Gracias, Navidad, etc.), que este cálculo NO resta del calendario esperado (a propósito, está documentado en el comentario del archivo: "holidays are intentionally not modeled"). Es una simplificación consciente, no un bug — modelar el calendario exacto de festivos de NYSE hubiera sido trabajo extra sin valor real para este proyecto.

**`mart_asset_summary.sql`** — el resumen "tarjetero" de cada activo:

```sql
v.daily_return_stddev_30d * sqrt(case when l.symbol like '%-USD' then 365 else 252 end) as volatility_30d
```

**Qué hace exactamente:** esto es la **anualización de volatilidad**, un concepto financiero estándar: la desviación estándar de los retornos diarios se multiplica por la raíz cuadrada del número de periodos en un año para expresarla en términos anuales comparables. Para cripto usamos 365 (opera todos los días); para acciones, 252 (los días hábiles bursátiles promedio al año). Este mismo número (252 vs 365) también se usa para anualizar el CAGR y el Sharpe en el backtester — es consistente en todo el proyecto, y está en `config.py` como `periods_per_year`.

### 4.4 Las 33 pruebas de dbt — clasificadas por qué protegen

No son 33 pruebas al azar — se agrupan en 5 familias, cada una defendiendo contra un tipo distinto de error:

**Familia 1 — Integridad de identidad (5 pruebas, tipo `unique`/`unique_combination`):**
```
unique_combination_stg_ohlcv_symbol__granularity__ts
unique_combination_fct_ohlcv_indicators_symbol__ts
unique_combination_mart_source_reconciliation_symbol__ts
unique_mart_asset_summary_symbol
unique_mart_data_quality_symbol
```
Verifican que no exista más de una fila para la misma llave. El test genérico que las implementa (`dbt/tests/generic/unique_combination.sql`) es simple y elegante:
```sql
select {{ combination | join(', ') }}, count(*) as n_rows
from {{ model }} group by {{ combination | join(', ') }} having count(*) > 1
```
Si hay algún grupo con más de 1 fila, la query devuelve resultados → dbt interpreta "hay filas" como test fallido (la convención de dbt: un test SQL falla si devuelve AL MENOS una fila).

**» NEGOCIO:** esto es lo que garantiza que tu curva de precio de BTC-USD nunca tenga dos valores distintos para el mismo día — que sería catastrófico para cualquier gráfico o backtest que la consuma.

**Familia 2 — Completitud (12 pruebas, tipo `not_null`):**
```
not_null_stg_ohlcv_all_sources_{source,symbol,ts,open,high,low,close,volume}   (7)
not_null_stg_ohlcv_{source,symbol,ts}                                            (3)
not_null_fct_ohlcv_indicators_{symbol,ts}                                        (2)
not_null_mart_source_reconciliation_{symbol,ts}                                  (2)
not_null_mart_asset_summary_symbol, not_null_mart_data_quality_symbol            (2)
```
Verifican que columnas críticas nunca vengan vacías. Las 5 de `open/high/low/close/volume` en `stg_ohlcv_all_sources` son las que **agregamos anoche** durante la revisión adversarial — antes no existían, y son justamente las que hubieran detectado el Error 3 (Kraken parseado mal) en el momento en que ocurrió, no horas después por casualidad.

**» NEGOCIO:** esta es la lección más cara de la noche, convertida en regla permanente: "si un precio viene vacío, que truene la construcción del pipeline ahí mismo, con un mensaje claro de qué modelo y qué columna — no que el vacío se cuele silenciosamente y aparezca como un misterio 3 pasos después en el backtest".

**Familia 3 — Valores permitidos (3 pruebas, tipo `accepted_values`):**
```
accepted_values_stg_ohlcv_all_sources_source__coinbase__kraken__tiingo
accepted_values_stg_ohlcv_source__coinbase__kraken__tiingo
accepted_values_mart_data_quality_asset_class__crypto__equity
```
Verifican que una columna de texto solo contenga uno de los valores esperados de una lista cerrada. Si algún día un bug escribiera `source = 'Coinbase'` (con mayúscula) o `'coinbse'` (typo), esta prueba lo atrapa inmediatamente.

**Familia 4 — Consistencia lógica de negocio (1 prueba, `ohlc_consistency`):**
```sql
{% test ohlc_consistency(model) %}
select * from {{ model }}
where high < low or close < low or close > high
{% endtest %}
```
**Qué hace exactamente:** en una vela válida, matemáticamente el máximo del día (`high`) siempre debe ser ≥ el mínimo (`low`), y el cierre siempre debe estar dentro de ese rango `[low, high]`. Si alguna fila viola esa regla física básica, algo está corrupto en el origen.

**» NEGOCIO:** esta es una regla de sentido común financiero traducida a código: es imposible que el precio de cierre esté por encima del máximo del día — si eso pasara, o la fuente de datos está rota, o hay un bug de parseo. Esta prueba es tu "detector de mentiras" automático sobre la coherencia física de los precios.

**Familia 5 — Salud del sistema (2 pruebas de comportamiento distinto):**
- **`assert_reconciliation_discrepancy_rate`** — un test con `config(severity='warn')`, no `error`: si el 10% o más de las velas comparadas Coinbase-vs-Kraken son discrepantes (>0.5%), **avisa pero no rompe la construcción**. La diferencia `warn` vs `error` es deliberada: una tasa alta de discrepancia apunta a un problema sistemático de una fuente que merece investigación humana, pero no es lo suficientemente grave como para detener todo el pipeline automáticamente.
- **Freshness check** sobre `bronze.raw_candles` (declarado en `sources.yml`): `warn_after: 36 hours`, `error_after: 72 hours`. Esto no revisa el CONTENIDO de los datos, revisa el RELOJ — cuánto tiempo pasó desde la última vez que se insertó algo. Si el cron diario deja de correr por 3 días sin que nadie note, este check te lo grita.

**» NEGOCIO:** la distinción `warn` vs `error` es una decisión de gestión de riesgo real: algunas anomalías merecen parar la línea de producción inmediatamente (un precio físicamente imposible), otras solo merecen una nota para revisión humana (una discrepancia de fuente que podría ser ruido normal de mercado). Tratarlas todas igual —o todas como fatales, o todas como advertencias— sería perder esa información.

---

## 5. El backtester — el motor de simulación, mecánica exacta

### 5.1 Las 4 estrategias — la definición matemática exacta de cada una

```python
# SmaCross — cruce de medias móviles
def generate_signals(self, df):
    return (_sma_of_close(df, self.fast) > _sma_of_close(df, self.slow)).astype(int)
```
Posición larga (1) mientras la media rápida (20 días, en tu config) esté por encima de la lenta (50 días); plana (0) en caso contrario. Es la estrategia técnica más clásica que existe — el "golden cross / death cross" que ves mencionado en cualquier análisis de mercado.

```python
# Macd
macd_line = close.ewm(span=self.fast, adjust=False).mean() - close.ewm(span=self.slow, adjust=False).mean()
signal_line = macd_line.ewm(span=self.signal, adjust=False).mean()
return (macd_line > signal_line).astype(int)
```
`ewm` = *exponentially weighted moving average* — a diferencia del SMA (que pesa todos los días de la ventana por igual), una EMA le da más peso a los datos recientes, con el peso decayendo exponencialmente hacia atrás. `adjust=False` es un detalle de implementación de pandas: usa la fórmula recursiva clásica (`EMA_t = α·precio_t + (1-α)·EMA_{t-1}`) en vez de un promedio ponderado normalizado — es la convención estándar de la industria para MACD.

```python
# RsiReversion — la única con estado (loop explícito, no vectorizada)
for i, value in enumerate(rsi):
    if position == 0 and value < self.entry_below:
        position = 1
    elif position == 1 and value > self.exit_above:
        position = 0
    out[i] = position
```
**Por qué esta es la única con un `for` en vez de una operación vectorizada de pandas:** las otras 3 estrategias son "sin memoria" — el signal del día de hoy solo depende de los indicadores de hoy. Esta SÍ tiene memoria: "si ya estoy dentro (position==1), me quedo dentro hasta que el RSI suba de 50, sin importar que baje y suba de nuevo por debajo de 30 mientras tanto". Eso es un estado que se acumula día a día, así que necesita el loop explícito.

```python
# VolumeBreakout
above_price = df["close"] > sma(df["close"], self.price_window)
above_volume = df["volume"] > self.volume_mult * sma(df["volume"], self.volume_window)
return (above_price & above_volume).astype(int)
```
Exige DOS condiciones simultáneas: precio rompiendo su media Y volumen 1.5× por encima de su propia media — la idea detrás es que un movimiento de precio "confirmado" por volumen alto es más confiable que uno con volumen normal (podría ser ruido).

**» NEGOCIO:** estas 4 reglas son deliberadamente simples y de libro de texto — no inventamos indicadores exóticos. Eso es una decisión correcta: el valor del proyecto no está en "inventar la estrategia secreta ganadora" (eso no existe de forma consistente, y cualquiera que lo prometa está mintiendo), está en la **honestidad de la medición**: aplicar reglas conocidas con rigor metodológico y reportar sin filtro lo que realmente pasó, incluidas las estrategias que perdieron.

### 5.2 El motor — por qué el `for` loop bar-a-bar, y qué es "next-open execution"

```python
# pipeline/backtest/engine.py
for t in range(n):
    target = int(sig[t - 1]) if t > 0 else 0
    if target == 1 and units == 0.0:
        entry_fill = opens[t] * (1.0 + slip)
        units = cash / (entry_fill * (1.0 + fee))
        cash = 0.0
    elif target == 0 and units > 0.0:
        exit_fill = opens[t] * (1.0 - slip)
        cash = units * exit_fill * (1.0 - fee)
    equity[t] = cash + units * closes[t]
```

**La línea más importante de todo el proyecto es `target = int(sig[t - 1]) if t > 0 else 0`.** Léela con cuidado: en la barra `t`, la posición que se ejecuta es la señal generada en la barra `t-1` (la anterior), no la señal de `t`. Y se ejecuta contra `opens[t]` — el precio de APERTURA de la barra actual, no el cierre.

**Qué significa esto en la práctica:** si el 15 de marzo, al CIERRE del día, tu estrategia calcula "señal = comprar", esa compra se simula ejecutándose a la APERTURA del 16 de marzo — nunca al cierre del 15, porque en la vida real, cuando el mercado cerró el 15 y calculaste tu señal, ya no podías comprar al precio de cierre del 15 (ya pasó). Esto se llama evitar el **look-ahead bias** (sesgo de mirar al futuro): el error de simular una operación con información que en ese instante todavía no existía.

`opens[t] * (1.0 + slip)` — al comprar, pagas un poco más del precio de apertura (slippage adverso); al vender, `opens[t] * (1.0 - slip)` — recibes un poco menos. `units = cash / (entry_fill * (1.0 + fee))` — el fee reduce las unidades que puedes comprar con el mismo efectivo (equivalente a pagar comisión sobre el monto).

**» NEGOCIO:** este es, sin exagerar, el 80% del valor técnico de todo el backtester. La mayoría de "backtests" caseros que ves en YouTube o foros cometen look-ahead bias sin darse cuenta — simulan comprar al precio de cierre del mismo día en que la señal se generó con ese cierre, lo cual es imposible en la realidad. Ese error sistemáticamente infla los resultados simulados (a veces dramáticamente), y es la razón #1 por la que estrategias que "funcionaban perfecto" en el backtest pierden dinero real cuando se operan en vivo. Que este motor lo evite por construcción — no como un parche, sino como la arquitectura central del loop — es la diferencia entre un ejercicio de aula y una herramienta que un profesional tomaría en serio.

### 5.3 Las métricas — sus fórmulas exactas

```python
def max_drawdown(equity):
    return float((equity / equity.cummax() - 1.0).min())
```
`equity.cummax()` = el máximo acumulado hasta cada punto ("el pico más alto que he tocado hasta ahora"). `equity / cummax() - 1` = cuánto por debajo estoy de mi propio pico histórico, en cada punto. El `.min()` de esa serie es el peor momento — la caída más profunda desde cualquier pico hasta el valle siguiente. Siempre es ≤ 0.

```python
def sharpe(equity, periods_per_year):
    returns = equity.pct_change().dropna()
    if len(returns) < 2:
        return None
    std = float(returns.std(ddof=1))
    if std == 0.0 or math.isnan(std):
        return None
    return float(returns.mean() / std) * math.sqrt(periods_per_year)
```
`equity.pct_change()` = el retorno porcentual de cada barra respecto a la anterior. `returns.mean() / returns.std()` = retorno promedio por unidad de riesgo (volatilidad) — el Sharpe ratio sin anualizar. Multiplicar por `sqrt(periods_per_year)` lo anualiza (esta es la fórmula estándar de la industria para anualizar Sharpe a partir de retornos de mayor frecuencia). `ddof=1` en `.std()` es desviación estándar **muestral** (aquí sí, a propósito — porque estás estimando el riesgo de una muestra de retornos, no describiendo una población completa conocida, que es la distinción correcta entre `stddev_samp` y `stddev_pop` que vimos en Bollinger).

**» NEGOCIO:** el `if std == 0.0: return None` no es un detalle menor — significa "si la estrategia nunca se movió (por ejemplo, se quedó plana todo el periodo), el Sharpe no está definido matemáticamente (división por cero), así que reportamos `None` (ausente) en vez de inventar un número". Preferir `None` a un número falso es el mismo principio de honestidad que vimos con los indicadores NULL durante el warm-up.

### 5.4 `pipeline/quality.py` — el portero antes del backtest

```python
ohlcv_schema = pa.DataFrameSchema(
    columns={
        "open": pa.Column(float, pa.Check.gt(0)),
        "rsi_14": pa.Column(float, pa.Check.in_range(0, 100), nullable=True),
        ...
    },
    checks=[
        pa.Check(lambda df: df["high"] >= df["low"], error="high < low"),
        pa.Check(lambda df: df["ts"].is_monotonic_increasing, error="ts not sorted"),
        ...
    ],
)
```

**Qué hace exactamente:** esto es **Pandera**, una librería que valida DataFrames de pandas con un esquema declarativo — como un "contrato de forma" para tus datos. `pa.Check.gt(0)` exige que `open` sea estrictamente positivo; `in_range(0,100)` exige que el RSI esté en su rango matemáticamente válido; el chequeo de `is_monotonic_increasing` exige que las fechas vengan ordenadas cronológicamente.

**» NEGOCIO:** dbt ya valida la calidad de los datos DENTRO de Postgres (las 33 pruebas de la sección 4). Este es un **segundo candado, independiente, justo antes de que los datos entren al backtester** — la idea es que si por cualquier motivo (un bug futuro, una migración mal hecha) llegara un dato corrupto hasta este punto, el backtester se niega a correr con él en vez de producir un resultado silenciosamente incorrecto que alguien podría tomar por bueno. Es defensa en profundidad: dos capas de control independientes, no una sola.

---

## 6. Los 3 errores reales que encontramos anoche — con el código exacto del antes/después

Esto es lo más valioso de todo el documento: bugs reales, en código real, encontrados por un proceso de revisión adversarial (10 agentes de IA distintos revisando el trabajo de los primeros 4, buscando específicamente fallas), no hipotéticos de manual.

### 6.1 Error — Watermark envenenado por datos de prueba

**Síntoma:** la tabla `meta.ingest_runs` mostró 3 filas con `started_at` = exactamente `00:00:00.000000` — algo que un proceso real, corriendo con la hora del sistema, físicamente no puede producir (siempre hay microsegundos de por medio).

**Causa raíz:** mientras uno de los agentes de IA construía y probaba el módulo de dbt en paralelo, insertó datos de prueba (130 velas sintéticas hasta el 2025-07-09) directamente en tu Postgres local para poder correr `dbt build` sin depender de que la ingesta real ya hubiera corrido. El problema: cuando el flujo real corrió después, la query del watermark (`SELECT max(candle_ts) ...`) encontró esas 130 velas de prueba y concluyó "ya tengo datos hasta 2025-07-09, solo pido desde el día siguiente" — saltándose 3+ años de historia real (2022 a mediados de 2025).

**Cómo lo detectamos:** revisando manualmente `meta.ingest_runs` después de la primera corrida completa, el patrón de timestamps idénticos fue la pista.

**Fix aplicado:**
```sql
TRUNCATE bronze.raw_candles; TRUNCATE meta.ingest_runs CASCADE;
```
Vaciamos ambas tablas y volvimos a correr la ingesta completa desde cero, esta vez sin contaminación.

**» NEGOCIO:** la lección permanente que dejamos escrita en el test correspondiente (`test_seed_bronze_sql_applies_to_local_pg`) es: **cualquier dato sintético de prueba debe aplicarse dentro de una transacción con rollback garantizado, nunca directamente contra una base que el proceso real también usa**. Es exactamente la razón por la que, en un entorno contable serio, nunca pruebas asientos ficticios directamente en el libro de producción — usas un ambiente de pruebas separado, o reviertes inmediatamente.

### 6.2 Error — Las fechas del export corridas un día (timezone)

**Síntoma:** la primera vez que revisamos el JSON exportado, la primera vela de Bitcoin aparecía fechada `2021-12-31`, un día ANTES de tu `backfill_start` configurado (`2022-01-01`) — una imposibilidad lógica que saltó a la vista.

**Causa raíz exacta**, código real, antes:
```python
# pipeline/export.py — ANTES
with psycopg.connect(database_url()) as conn:
    ...
    points = [[c["ts"].date().isoformat(), ...] for c in curve]
```

`psycopg` (el driver de Python para Postgres) devuelve las columnas `TIMESTAMPTZ` como objetos `datetime` **en la zona horaria configurada en la sesión del servidor**, no necesariamente en UTC. Tu servidor Postgres local tenía configurado `TimeZone = America/Bogota` (UTC-5). Entonces un instante guardado como `2022-01-01 00:00:00 UTC` llegaba a Python como `2021-12-31 19:00:00-05:00` (mismo instante exacto, representado en hora de Bogotá). `.date()` sobre ese objeto te da `2021-12-31` — la fecha civil de Bogotá, no la de UTC.

**Fix aplicado:**
```python
# pipeline/export.py — DESPUÉS
with psycopg.connect(database_url()) as conn:
    conn.execute("SET TIME ZONE 'UTC'")   # ← la línea que arregla todo
    ...
```
Con la sesión forzada a UTC, `psycopg` devuelve los `datetime` ya en UTC, y `.date()` da la fecha correcta.

**Verificación real, antes vs. después (recorriendo el JSON exportado):**
```
ANTES:   primer punto = 2021-12-31   último punto = 2026-08-15
DESPUÉS: primer punto = 2022-01-01   último punto = 2026-08-16
```

**» NEGOCIO:** este bug es sutil precisamente porque **no rompe nada visiblemente** — el archivo se genera sin errores, los números son correctos, solo la ETIQUETA de fecha de cada punto está corrida un día. Es el tipo de error que un revisor humano fácilmente pasaría por alto (¿quién cuenta manualmente si un gráfico de 400 puntos empieza el día correcto?), pero que sería vergonzoso si un reclutador técnico lo notara en tu portafolio. La regla general que queda aprendida: **cualquier conexión que vaya a convertir fechas a texto debe fijar explícitamente su zona horaria a UTC, sin depender de la configuración del servidor donde corra** — porque ese servidor puede cambiar (tu Postgres local vs. Supabase en la nube probablemente tengan configuraciones de zona horaria distintas).

### 6.3 Error — Kraken parseado como si fuera un array, cuando en realidad es un diccionario

**Síntoma:** todas las columnas de precio (`open`, `high`, `low`, `close`, `volume`) para las velas de Kraken en la capa `silver` salían `NULL` — cero excepciones, cero errores visibles, solo vacíos silenciosos.

**Causa raíz:** dos agentes de IA distintos trabajaron en paralelo esa noche — uno construyó `pipeline/sources/kraken.py` (el cliente que llama la API y guarda el `payload`), otro construyó el modelo dbt que lee ese `payload` y lo convierte a columnas. El cliente de Python SÍ etiqueta correctamente el payload con nombres (`dict(zip(_FIELDS, row, strict=True))`, como viste en la sección 1.3) — guarda `{"time": ..., "open": ..., "high": ..., ...}`, un diccionario con claves. Pero el modelo dbt, escrito por el otro agente sin ver el código Python real, asumió que el `payload` de Kraken se había guardado como un **array posicional** sin nombres, e intentaba leerlo así:

```sql
-- dbt/models/staging/stg_ohlcv_all_sources.sql — ANTES (incorrecto)
case source
    when 'coinbase' then (payload ->> 'open')::numeric
    when 'kraken'   then (payload ->> 1)::numeric        -- ← payload ->> 1 : "dame el elemento en la posición 1"
    when 'tiingo'   then (payload ->> 'open')::numeric
end as open,
```

`payload ->> 1` es sintaxis válida de Postgres para JSON — pero es para sacar el elemento en la posición 1 de un ARRAY JSON (`["a","b","c"]`), no una clave de un diccionario. Como el payload real de Kraken era `{"open": "63000.5", ...}` (un objeto, no un array), `payload ->> 1` simplemente no encontraba nada que extraer, y Postgres devuelve `NULL` sin lanzar ningún error — SQL no te avisa "esto no tiene sentido", solo te da vacío.

**Fix aplicado:**
```sql
-- DESPUÉS (correcto, y más simple)
(payload ->> 'open')::numeric   as open,
(payload ->> 'high')::numeric   as high,
(payload ->> 'low')::numeric    as low,
(payload ->> 'close')::numeric  as close,
(payload ->> 'volume')::numeric as volume,
```
Como las tres fuentes en realidad SÍ guardan diccionarios con las mismas claves nombradas (fue una decisión de diseño explícita en `sources/base.py` — "labels them for the bronze payload", dice el comentario del código), el `CASE WHEN source = ...` completo era innecesario: una sola línea por columna sirve para las tres fuentes.

**Cómo lo detectamos:** corrimos `dbt build` contra datos reales y una simple query de verificación (`SELECT count(*) FILTER (WHERE open IS NULL) FROM silver.stg_ohlcv_all_sources GROUP BY source`) mostró 100% de nulos para `kraken`, 0% para las otras dos — la asimetría fue la pista inmediata.

**» NEGOCIO:** esta es la lección de "contratos entre componentes deben verificarse contra la forma REAL del dato, nunca contra una suposición" — cuando dos personas (o dos agentes de IA) construyen piezas que se conectan sin ver el código exacto del otro lado, es fácil que cada uno asuma una forma de datos ligeramente distinta y ambas partes "compilen" sin error porque SQL/JSON son permisivos con estructuras que no encajan (te dan NULL en vez de gritar). Es exactamente el mismo riesgo que corres cuando dos áreas de una empresa intercambian un archivo Excel sin un formato estrictamente acordado: "yo asumí que la columna C era la fecha, tú la llenaste como texto libre" — funciona hasta que alguien nota que los cálculos posteriores están mal. La corrección permanente no fue solo arreglar la query: fueron los 5 tests `not_null` en `open/high/low/close/volume` que agregamos (sección 4.4, Familia 2) — para que la PRÓXIMA vez que algo así ocurra, `dbt build` truene inmediatamente con un mensaje claro, en vez de que el error se descubra por casualidad revisando manualmente.

---

## 7. Orquestación — Prefect + GitHub Actions, el flujo completo

```python
# pipeline/flows.py
@flow(name="daily-medallion-flow", log_prints=True)
def daily_flow():
    cfg = load_config()
    for asset in cfg.assets:
        sources = [asset.sources.primary]
        if asset.sources.reconcile:
            sources.append(asset.sources.reconcile)
        for source_name in sources:
            if source_name == "tiingo" and not tiingo_api_key():
                logger.warning("TIINGO_API_KEY not set — skipping...")
                continue
            ingest_asset(source_name, asset, cfg.granularity)   # @task

    run_dbt()                                                    # @task

    for asset in cfg.assets:
        backtest_symbol(asset, cfg)                              # @task

    path = export.export_json(cfg)
```

**Qué hace exactamente:** un `@flow` de Prefect es la función "maestra"; cada `@task` dentro es un paso independiente que Prefect trackea por separado (con su propio log, su propio estado de éxito/fallo, y — mira `ingest_asset` — reintentos configurables: `@task(retries=2, retry_delay_seconds=30)`). El orden es secuencial y con dependencia lógica clara: no tiene sentido correr dbt antes de que la ingesta termine, ni backtestear antes de que dbt haya calculado los indicadores.

**» NEGOCIO:** Prefect aquí no está haciendo nada que un script con funciones normales no pudiera hacer en teoría — la diferencia es la **observabilidad**: cada paso queda registrado con su duración, su resultado, sus reintentos, visible en un dashboard si conectas Prefect Cloud (gratis). Es la diferencia entre un script que "corrió o no corrió" (una caja negra) y un proceso donde puedes ver exactamente en qué paso falló y por qué, sin tener que adivinar leyendo logs de texto plano.

```yaml
# .github/workflows/daily.yml
on:
  schedule:
    - cron: "47 10 * * *"   # 10:47 UTC todos los días
  workflow_dispatch:         # + botón para correrlo manualmente

jobs:
  refresh:
    steps:
      - name: Check required secrets
        id: guard
        run: |
          if [ -z "$DATABASE_URL" ]; then
            echo "configured=false" >> "$GITHUB_OUTPUT"
          else
            echo "configured=true" >> "$GITHUB_OUTPUT"
          fi
      - name: Run daily pipeline flow
        if: steps.guard.outputs.configured == 'true'
        run: uv run python -m pipeline.flows
      ...
      - name: Commit refreshed exports
        run: |
          git add exports/*.json
          if git diff --cached --quiet; then
            echo "exports unchanged, nothing to commit"
          else
            git commit -m "data: daily refresh [skip ci]"
            git push
          fi
```

**Qué hace exactamente:** GitHub Actions es el "programador de tareas en la nube" de GitHub. `cron: "47 10 * * *"` usa la sintaxis estándar de cron (minuto, hora, día del mes, mes, día de la semana) — corre a las 10:47 UTC (elegido a propósito en un minuto "raro", no en punto, porque GitHub retrasa más los crons que caen justo a la hora exacta, cuando todo el mundo programa sus tareas). El paso "Check required secrets" es un **guard clause**: si no configuraste los 4 secrets todavía (que es tu situación actual), el resto de los pasos se saltan limpiamente en vez de fallar con un error confuso. El último paso hace `git commit` y `git push` **desde el propio runner de GitHub** — el robot se commitea a sí mismo, literalmente, con el mensaje `[skip ci]` para evitar que ese commit dispare otra corrida de CI en cadena infinita.

**» NEGOCIO:** esto es lo único que falta para que tu "historial verde" de build-in-public empiece a acumularse solo — necesitas configurar 4 secrets en GitHub (Settings → Secrets → Actions del repo): `DATABASE_URL` (el connection string de tu Supabase), `TIINGO_API_KEY`, `SUPABASE_URL` y `SUPABASE_ANON_KEY` (estos dos últimos para el ping de keep-alive que evita que Supabase pause tu base por inactividad). Una vez configurados, cada mañana a las 10:47 UTC el robot va a: traer las velas nuevas de ayer, recalcular todo dbt, correr los 16 backtests de nuevo, y commitear el JSON actualizado — sin que tú hagas nada.

---

## 8. Glosario técnico de referencia rápida

| Término | Definición precisa |
|---|---|
| **Watermark** | El punto (típicamente una fecha/timestamp) hasta el cual un proceso incremental ya procesó datos; determina dónde retomar la próxima corrida |
| **Idempotencia** | Propiedad de una operación tal que ejecutarla N veces produce el mismo estado final que ejecutarla 1 vez |
| **Transacción (DB)** | Un bloque de operaciones que se aplican todas juntas o ninguna (atomicidad); revierte ("rollback") si algo falla a mitad de camino |
| **Window function (SQL)** | Función que calcula un valor sobre un conjunto de filas relacionadas con la fila actual (`PARTITION BY` + `ORDER BY`), sin colapsar el resultado a una sola fila como haría un `GROUP BY` |
| **ROWS vs RANGE (framing)** | Dos formas de definir el límite de una ventana SQL: por conteo físico de filas, o por rango de valores — pueden dar resultados distintos con fechas duplicadas |
| **Look-ahead bias** | Error de simular una decisión usando información que, en el instante real de esa decisión, todavía no existía |
| **Slippage** | La diferencia entre el precio que ves al decidir y el precio real al que se ejecuta la orden |
| **EMA vs SMA** | Media exponencial (pesa más lo reciente, decae exponencialmente) vs media simple (pesa todo igual dentro de la ventana) |
| **Backoff exponencial** | Estrategia de reintento donde la espera entre intentos se duplica cada vez (1s, 2s, 4s...) |
| **JSONB** | Tipo de columna en Postgres que guarda JSON en formato binario indexable |
| **dbt model** | Un archivo `.sql` que define una tabla/vista derivada, con dependencias explícitas vía `{{ ref() }}` |
| **dbt test** | Una query SQL que, si devuelve alguna fila, se interpreta como una falla de calidad de datos |
| **Severity (dbt)** | `error` detiene el build; `warn` solo notifica — nivel de urgencia configurable por test |
| **Freshness check (dbt)** | Prueba que mide cuánto tiempo pasó desde la última actualización de una fuente, no su contenido |
| **Pandera** | Librería de Python que valida la forma y reglas de un DataFrame contra un esquema declarativo |
| **Prefect flow/task** | Unidad de trabajo orquestada, con reintentos y logging propios, dentro de un pipeline |
| **Cron** | Sintaxis estándar (`minuto hora día mes día-semana`) para programar tareas recurrentes |
| **Guard clause** | Verificación temprana que corta la ejecución (o la salta limpiamente) si una precondición no se cumple |

---

*Documento vivo — si agregamos activos, estrategias o fuentes nuevas, esta bitácora se actualiza junto con el código que describe.*
