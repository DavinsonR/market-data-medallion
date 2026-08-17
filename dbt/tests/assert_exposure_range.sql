-- exposure is the fraction of bars the variant held a non-zero position, so it must live
-- in [0, 1]. A value outside the range means the engine divided by the wrong bar count.
-- NULL is allowed and is not a failure: it means the run predates migration 003 and simply
-- never measured exposure. Written as a singular test because accepted_range only exists in
-- dbt_utils, which this project deliberately does not install.
select
    symbol,
    strategy,
    exposure
from {{ ref('mart_combination_analysis') }}
where exposure is not null
    and (exposure < 0 or exposure > 1)
