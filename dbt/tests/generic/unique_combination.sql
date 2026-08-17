{% test unique_combination(model, combination) %}

-- Fails when more than one row shares the same combination of key columns.
select
    {{ combination | join(', ') }},
    count(*) as n_rows
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1

{% endtest %}
