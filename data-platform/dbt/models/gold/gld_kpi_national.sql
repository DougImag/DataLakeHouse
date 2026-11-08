{#
  Indicateurs nationaux par année, pour les tuiles KPI du dashboard.
  L'année de référence pré-Covid est paramétrable :
    dbt build --vars '{annee_reference: 2019}'
#}

{%- set annee_reference = var('annee_reference', 2019) -%}

with agregat as (

    select
        annee,
        count(*)                                        as nb_gares,
        sum(nb_voyageurs)                               as nb_voyageurs,
        count_if(evolution_pct > 0)                     as nb_gares_en_hausse,
        count_if(evolution_pct < 0)                     as nb_gares_en_baisse
    from {{ ref('gld_frequentation_gares') }}
    group by annee

),

calculs as (

    select
        *,
        lag(nb_voyageurs) over (order by annee)        as nb_voyageurs_annee_precedente,
        lag(annee)        over (order by annee)        as annee_precedente,
        max(case when annee = {{ annee_reference }} then nb_voyageurs end) over () as nb_voyageurs_reference
    from agregat

)

select
    annee,
    nb_gares,
    nb_voyageurs,
    case when annee_precedente = annee - 1
         then round((nb_voyageurs - nb_voyageurs_annee_precedente) * 100.0
                    / nullif(nb_voyageurs_annee_precedente, 0), 2)
    end                                                              as evolution_pct,
    {{ annee_reference }}                                            as annee_reference,
    round((nb_voyageurs - nb_voyageurs_reference) * 100.0
          / nullif(nb_voyageurs_reference, 0), 2)                    as evolution_vs_reference_pct,
    nb_gares_en_hausse,
    nb_gares_en_baisse
from calculs
