-- The decomposition identity (1 + r_local) = (1 + r_usd) * (1 + r_fx) must hold exactly
-- for every row that carries all three returns; a violation means the mart's arithmetic
-- (or a sign convention) broke. Rows with any NULL return are windows one series lacks —
-- absent on purpose, not failures. 1e-9 tolerates float noise only.
select
    symbol,
    window_label,
    usd_return,
    fx_return,
    local_return,
    abs((1 + usd_return) * (1 + fx_return) - (1 + local_return)) as identity_error
from {{ ref('mart_fx_decomposition') }}
where usd_return is not null
    and fx_return is not null
    and local_return is not null
    and abs((1 + usd_return) * (1 + fx_return) - (1 + local_return)) >= 1e-9
