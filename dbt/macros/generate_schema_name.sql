{#
    Override dbt's default schema naming so models land in the EXACT schema
    given by their +schema config (silver / gold) instead of the default
    "<target.schema>_<custom_schema>" concatenation.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
