-- The project's headline honesty metric: of the variants that beat buy and hold on the
-- data they were selected on, how many kept beating it on data they never touched?
--
-- Searching ~1,347 variants guarantees that some look brilliant by chance alone, and the
-- more strategies a combination ANDs together the more degrees of freedom it has to fit
-- noise — which is exactly why this is aggregated by n_components.
--
-- GRAIN — one row per n_components (1..5) plus one overall row:
--   is_grand_total = false -> n_components is the combination size
--   is_grand_total = true  -> n_components is NULL, every variant pooled
-- Both levels come from the SAME population (mart_combination_analysis, the latest run
-- per symbol and strategy), so the overall row is an exact aggregate of the detail rows.
--
-- DENOMINATORS — the in/out-of-sample shares divide by the number of variants whose
-- window was actually evaluated, not by n_variants. A window the engine refused to score
-- (too few bars) is missing evidence, and counting it as a loss would flatter the
-- out-of-sample numbers of whatever did get scored.

with variants as (

    select * from {{ ref('mart_combination_analysis') }}

),

aggregated as (

    select
        grouping(n_components) = 1 as is_grand_total,
        case when grouping(n_components) = 1 then null else n_components end as n_components,
        count(*)::int as n_variants,
        count(excess_return)::int as n_evaluated_full,
        count(is_excess_return)::int as n_evaluated_is,
        count(oos_excess_return)::int as n_evaluated_oos,
        count(oos_excess_drop)::int as n_evaluated_both,
        count(*) filter (where beat_bh_full)::int as n_beat_full,
        count(*) filter (where beat_bh_is)::int as n_beat_is,
        count(*) filter (where beat_bh_is and oos_excess_return is not null)::int
            as n_beat_is_scored_oos,
        count(*) filter (where beat_bh_oos)::int as n_beat_oos,
        count(*) filter (where beat_bh_is and beat_bh_oos)::int as n_beat_is_and_oos,
        avg(exposure) as avg_exposure,
        avg(excess_return) as avg_excess_return,
        avg(is_excess_return) as avg_is_excess_return,
        avg(oos_excess_return) as avg_oos_excess_return,
        avg(oos_excess_drop) as avg_oos_excess_drop
    from variants
    group by grouping sets ((n_components), ())

)

select
    is_grand_total,
    n_components,
    n_variants,
    -- The denominators are published alongside the shares: a rate whose population cannot
    -- be inspected is not an honesty metric.
    n_evaluated_full,
    n_evaluated_is,
    n_evaluated_oos,
    n_evaluated_both,
    n_beat_full,
    n_beat_is,
    n_beat_is_scored_oos,
    n_beat_oos,
    n_beat_is_and_oos,
    n_beat_full::numeric / nullif(n_evaluated_full, 0) as share_beat_full,
    n_beat_is::numeric / nullif(n_evaluated_is, 0) as share_beat_is,
    n_beat_oos::numeric / nullif(n_evaluated_oos, 0) as share_beat_oos,
    n_beat_is_and_oos::numeric / nullif(n_evaluated_both, 0) as share_beat_is_and_oos,
    -- The survival rate: the share of the in-sample winners that stayed winners. The
    -- denominator counts only the in-sample winners whose out-of-sample window was actually
    -- scored — an unscored window is missing evidence, and letting it divide in as a death
    -- would make the strategies look worse than anything measured says they are. NULL, not
    -- zero, when nothing won in sample: there is then nothing to survive.
    n_beat_is_and_oos::numeric / nullif(n_beat_is_scored_oos, 0) as oos_survival_rate,
    avg_exposure,
    avg_excess_return,
    avg_is_excess_return,
    avg_oos_excess_return,
    avg_oos_excess_drop
from aggregated
order by is_grand_total, n_components
