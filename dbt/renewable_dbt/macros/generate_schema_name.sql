{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        When a model specifies schema='marts', we want it to land in
        WAGA.MARTS — not WAGA.STAGING_MARTS (dbt's default behavior
        of prefixing the target schema).

        If a custom schema is provided, use it directly.
        Otherwise fall back to the default target schema (STAGING).
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
