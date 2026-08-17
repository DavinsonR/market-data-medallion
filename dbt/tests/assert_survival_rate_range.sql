-- Every rate published by the out-of-sample marts is a share of a counted population, so
-- it must live in [0, 1], and the survivors can never outnumber the in-sample winners they
-- are drawn from. Either violation means a FILTER clause drifted away from its denominator
-- — the exact failure that would turn the project's honesty metric into a flattering
-- fiction. NULL rates are legitimate (nothing evaluated, or nothing won in sample).
with offenders as (

    select
        'mart_strategy_leaderboard' as relation,
        strategy || ' / ' || asset_class || ' / ' || region as grain,
        oos_survival_rate as rate
    from {{ ref('mart_strategy_leaderboard') }}
    where (oos_survival_rate is not null and (oos_survival_rate < 0 or oos_survival_rate > 1))
        or n_beat_is_and_oos > n_beat_is_scored_oos
        or n_beat_is_scored_oos > n_beat_is

    union all

    select
        'mart_overfitting_summary' as relation,
        coalesce(n_components::text, 'ALL') as grain,
        rate
    from {{ ref('mart_overfitting_summary') }}
    cross join lateral (
        values (share_beat_full), (share_beat_is), (share_beat_oos),
               (share_beat_is_and_oos), (oos_survival_rate)
    ) as r(rate)
    where (rate is not null and (rate < 0 or rate > 1))
        or n_beat_is_and_oos > n_beat_is_scored_oos
        or n_beat_is_scored_oos > n_beat_is
        or n_evaluated_both > least(n_evaluated_is, n_evaluated_oos)

)

select * from offenders
