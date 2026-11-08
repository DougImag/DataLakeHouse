{#
  Fréquentation annuelle par gare : passage des colonnes annuelles
  (total_voyageurs_2015 ... total_voyageurs_2024) en lignes.

  Les colonnes annuelles sont détectées automatiquement à partir du schéma
  de la table Bronze : une nouvelle année publiée par la SNCF est prise en
  compte sans modifier ce modèle. Le motif accepte aussi la variante sans
  underscore présente dans la source ("totalvoyageurs2017").
#}

{%- set relation = source('bronze', 'frequentation_sncf_v2') -%}
{%- set annees = [] -%}

{%- if execute -%}
    {%- set colonnes = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- for nom in colonnes -%}
        {%- set m = modules.re.match('^total_?voyageurs_?(\\d{4})$', nom) -%}
        {%- if m -%}
            {%- set annee = m.group(1) | int -%}
            {%- set nom_vnv = 'total_voyageurs_non_voyageurs_' ~ annee -%}
            {%- do annees.append({
                'annee': annee,
                'voyageurs': nom,
                'voyageurs_non_voyageurs': nom_vnv if nom_vnv in colonnes else none
            }) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if annees | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "Aucune colonne annuelle trouvée dans " ~ relation ~ " : " ~ colonnes | join(', ')
        ) }}
    {%- endif -%}
{%- else -%}
    {#- Phase de parsing : le schéma n'est pas lu, valeur fictive -#}
    {%- do annees.append({'annee': 0, 'voyageurs': 'nom_gare', 'voyageurs_non_voyageurs': none}) -%}
{%- endif -%}

{%- set annees = annees | sort(attribute='annee') %}

with source as (

    select * from {{ ref('int_bronze_derniere_ingestion') }}

),

pivot_inverse as (

    select
        trim(s.code_uic_complet)  as code_uic,
        t.annee,
        t.voyageurs_texte,
        t.voyageurs_non_voyageurs_texte,
        s.source_fichier,
        s.date_ingestion
    from source as s
    cross join unnest(
        array[{% for a in annees %}{{ a.annee }}{{ ', ' if not loop.last }}{% endfor %}],
        array[{% for a in annees %}s.{{ adapter.quote(a.voyageurs) }}{{ ', ' if not loop.last }}{% endfor %}],
        array[{% for a in annees %}{{ 's.' ~ adapter.quote(a.voyageurs_non_voyageurs) if a.voyageurs_non_voyageurs else 'cast(null as varchar)' }}{{ ', ' if not loop.last }}{% endfor %}]
    ) as t (annee, voyageurs_texte, voyageurs_non_voyageurs_texte)

)

select
    code_uic,
    cast(annee as integer)                                  as annee,
    {{ vers_bigint('voyageurs_texte') }}                    as nb_voyageurs,
    {{ vers_bigint('voyageurs_non_voyageurs_texte') }}      as nb_voyageurs_et_non_voyageurs,
    source_fichier,
    date_ingestion
from pivot_inverse
where {{ vers_double('voyageurs_texte') }} is not null
