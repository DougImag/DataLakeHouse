{#
  Fonctions de nettoyage des colonnes texte de la couche Bronze
  (toutes les colonnes y sont en VARCHAR).
#}

{# Chaîne vide ou composée d'espaces -> NULL #}
{% macro texte_ou_null(colonne) -%}
    nullif(trim({{ colonne }}), '')
{%- endmacro %}

{# Texte numérique ("1 234", "1234,5", "1234.0") -> double, NULL si invalide #}
{% macro vers_double(colonne) -%}
    try_cast(
        replace(replace(replace(trim({{ colonne }}), ' ', ''), U&'\00A0', ''), ',', '.')
        as double
    )
{%- endmacro %}

{# Texte numérique -> bigint arrondi, NULL si invalide #}
{% macro vers_bigint(colonne) -%}
    cast(round({{ vers_double(colonne) }}) as bigint)
{%- endmacro %}
