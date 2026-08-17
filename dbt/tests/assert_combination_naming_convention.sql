-- The naming convention is load-bearing, not cosmetic: a variant's strategy name is its
-- component names sorted alphabetically and joined by '+', n_components is the length of
-- that list, and strategy_kind is 'single' exactly when the length is 1. Everything
-- downstream relies on it — mart_strategy_leaderboard groups by strategy_kind and
-- n_components as attributes of the strategy name, and the exported JSON keys combinations
-- by that name. If the engine ever writes 'macd+sma_cross' with components
-- {sma_cross,macd}, the same combination would appear under two identities.
select
    symbol,
    strategy,
    strategy_kind,
    n_components,
    components
from {{ ref('mart_combination_analysis') }}
where strategy is distinct from array_to_string(components, '+')
    or n_components is distinct from cardinality(components)
    or strategy_kind is distinct from case
        when cardinality(components) = 1 then 'single' else 'combo' end
    -- ...and the components themselves must be in alphabetical order.
    or components is distinct from (select array_agg(c order by c) from unnest(components) as c)
