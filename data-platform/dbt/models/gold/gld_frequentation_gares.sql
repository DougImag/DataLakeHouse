{#
  Mart principal du dashboard : fréquentation par gare et par année,
  enrichie des attributs de la gare, de l'évolution annuelle et du
  classement national.
#}

with frequentation as (

    select * from {{ ref('slv_frequentation_annuelle') }}

),

gares as (

    select * from {{ ref('slv_gares') }}

),

calculs as (

    select
        f.code_uic,
        f.annee,
        f.nb_voyageurs,
        f.nb_voyageurs_et_non_voyageurs,
        lag(f.nb_voyageurs) over (partition by f.code_uic order by f.annee) as nb_voyageurs_annee_precedente,
        lag(f.annee)        over (partition by f.code_uic order by f.annee) as annee_precedente,
        sum(f.nb_voyageurs) over (partition by f.annee)                    as nb_voyageurs_national,
        rank()              over (partition by f.annee order by f.nb_voyageurs desc) as rang_national
    from frequentation as f

)

select
    c.code_uic,
    g.nom_gare,
    g.code_postal,
    g.code_departement,
    g.code_commune,
    g.latitude,
    g.longitude,
    coalesce(g.direction_regionale, 'Non renseignée')      as direction_regionale,
    coalesce(g.segmentation_drg, 'Non renseignée')         as segmentation_drg,
    coalesce(g.segmentation_marketing, 'Non renseignée')   as segmentation_marketing,
    c.annee,
    c.nb_voyageurs,
    c.nb_voyageurs_et_non_voyageurs,
    -- Évolution calculée uniquement si l'année précédente est bien N-1
    case when c.annee_precedente = c.annee - 1
         then c.nb_voyageurs_annee_precedente end           as nb_voyageurs_annee_precedente,
    case when c.annee_precedente = c.annee - 1
         then round(
             (c.nb_voyageurs - c.nb_voyageurs_annee_precedente) * 100.0
             / nullif(c.nb_voyageurs_annee_precedente, 0), 2)
    end                                                     as evolution_pct,
    c.rang_national,
    round(c.nb_voyageurs * 100.0 / nullif(c.nb_voyageurs_national, 0), 4) as part_nationale_pct
from calculs as c
inner join gares as g
    on g.code_uic = c.code_uic
