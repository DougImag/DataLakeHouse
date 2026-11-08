{#
  Utilise le schéma demandé tel quel ("silver", "gold") au lieu du
  comportement par défaut de dbt, qui le préfixe par le schéma cible
  (ex. "silver_gold").
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
