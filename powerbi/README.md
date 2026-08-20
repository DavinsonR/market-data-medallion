# Medallion Insights — Power BI report (PBIP)

An interactive Power BI report over the warehouse's gold marts, stored in the
**PBIP + PBIR + TMDL** developer formats: every part of the semantic model and the
report is plain text, versioned in git, and reviewable in a pull request — no
opaque binary. **No credentials live in this folder**: the database password is
entered once in Power BI Desktop and stays in your local credential store.

## Open it

1. Install [Power BI Desktop](https://aka.ms/pbidesktop) (free, no account needed
   to author). Any 2024+ release understands PBIP; if your build hides the
   formats behind preview switches, enable **File → Options → Preview features →
   "Power BI Project (.pbip) save option"** and **"Store reports using enhanced
   metadata format (PBIR)"**, then restart Desktop.
2. Open `powerbi/MedallionInsights.pbip` (File → Open report → Browse).
3. Desktop will ask to refresh. First time only, it prompts for credentials:
   - Connection: `aws-0-ca-central-1.pooler.supabase.com:5432` / database `postgres`
   - Authentication: **Database** (username + password)
   - Username: `postgres.trqwvhjugksmzzfmetbn`
   - Password: the Supabase database password
   - If asked about encryption, accept (the pooler requires TLS).
4. Refresh pulls ~270k rows (the equity curves dominate); expect a couple of
   minutes on a normal connection.

The connection host/database are model **parameters** (`PgHost`, `PgDatabase`),
so pointing the report at another Postgres — including the local dev warehouse at
`localhost:5433` / `mdm` — is Transform data → Edit parameters, no M editing.

## Pages

| Page | What it shows |
|---|---|
| **The Verdict** | The headline: variants evaluated → winners in-sample → survivors out-of-sample, with survival and market exposure by number of combined signals. The measures compute live, so slicing by region/class recomputes the funnel honestly. |
| **Strategy Explorer** | Region/class/kind slicers over all 1,347 variants: exposure-vs-OOS-excess scatter, the strategy leaderboard, and the full variant table. |
| **FX Decomposition** | The 10 Latin American ADRs split into company vs currency: `(1 + r_USD) × (1 + r_FX) = (1 + r_local)`, with a window slicer (30d/90d/365d/full). |
| **Equity Curves** | Strategy vs buy & hold over time for any asset — pick one symbol and one strategy in the slicers. |

The layout ships intentionally minimal (validated programmatically against
Microsoft's published PBIR JSON schemas — this machine has no Power BI to render
with). Restyle freely in Desktop; the semantic model underneath — relationships,
measures, format strings — is the part that carries the analysis.

## Model

```
dim_assets (48) ──< combination_analysis (1,347)   ← measures: survival, exposure, excess
            ├───< asset_summary (48)
            ├───< fx_decomposition (40)            ← measures: USD/local/FX move/drag
            └───< equity_curves (~270k)            ← measures: Equity, Buy & Hold Equity
overfitting_summary (standalone aggregate)
leaderboard (standalone aggregate)
```

Key measures (on `combination_analysis`): `Variants Evaluated`,
`Winners In-Sample`, `Winners IS & OOS`, `OOS Survival Rate`,
`Beat B&H % (OOS)`, `Avg Exposure`, `Avg OOS Excess Return`, `Median Sharpe`.

## Troubleshooting

- **"The remote certificate is invalid according to the validation procedure"** —
  the Supabase pooler presents a certificate signed by Supabase's own CA, which
  Windows does not trust out of the box. Clean fix: Supabase dashboard →
  Project Settings → Database → SSL → download the CA certificate, double-click
  it in Windows → Install Certificate → Current User → "Place all certificates
  in the following store" → **Trusted Root Certification Authorities** → finish,
  then refresh again. Quick-and-dirty alternative (unencrypted traffic — fine
  for public market data, your call): File → Options and settings → Data source
  settings → select the server → Edit Permissions → uncheck **Encrypt
  connections** and accept the warning.
- **"Couldn't resolve host"** — you are on a network without IPv4 internet or the
  pooler host changed; check `PgHost` in Edit parameters.
- **Login failed** — the database password rotates in Supabase → Settings →
  Database; update the credential under Transform data → Data source settings.
- **The report layout fails to load but the model opens** — create a blank report
  on the same model (File → New) and rebuild pages from the table above; the
  model, measures and relationships are the substance of this artifact.
