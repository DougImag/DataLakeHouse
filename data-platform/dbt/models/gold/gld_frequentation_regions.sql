{#
  Fréquentation par direction régionale et par année.
#}

with agregat as (

    select
        direction_regionale,
        annee,
        count(*)            as nb_gares,
        sum(nb_voyageurs)   as nb_voyageurs
    from {{ ref('gld_frequentation_gares') }}
    group by direction_regionale, annee

),

calculs as (

    select
        *,
        lag(nb_voyageurs) over (partition by direction_regionale order by annee) as nb_voyageurs_annee_precedente,
        lag(annee)        over (partition by direction_regionale order by annee) as annee_precedente,
        sum(nb_voyageurs) over (partition by annee)                              as nb_voyageurs_national
    from agregat

)

select
    direction_regionale,
    annee,
    nb_gares,
    nb_voyageurs,
    case when annee_precedente = annee - 1
         then round((nb_voyageurs - nb_voyageurs_annee_precedente) * 100.0
                    / nullif(nb_voyageurs_annee_precedente, 0), 2)
    end                                                                  as evolution_pct,
    round(nb_voyageurs * 100.0 / nullif(nb_voyageurs_national, 0), 2)   as part_nationale_pct,
    rank() over (partition by annee order by nb_voyageurs desc)         as rang_national
from calculs
