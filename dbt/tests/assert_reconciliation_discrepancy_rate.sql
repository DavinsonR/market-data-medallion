{{ config(severity='warn') }}

-- Warn when 10% or more of the cross-exchange bars are discrepant (> 0.5% close
-- difference): that points at a systematic source problem, not isolated noise.
with stats as (

    select
        count(*) filter (where is_discrepant)::numeric
            / nullif(count(*), 0) as discrepant_rate
    from {{ ref('mart_source_reconciliation') }}

)

select discrepant_rate
from stats
where discrepant_rate >= 0.10
