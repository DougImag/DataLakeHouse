{#
  Référentiel des gares : une ligne par gare (code UIC), colonnes typées.
#}

with source as (

    select * from {{ ref('int_bronze_derniere_ingestion') }}

),

geolocalisation as (

    select code_uic, latitude, longitude, code_commune
    from {{ ref('slv_gares_geolocalisation') }}

),

nettoyage as (

    select
        trim(code_uic_complet)                                   as code_uic,
        {{ texte_ou_null('nom_gare') }}                          as nom_gare,
        -- La source perd parfois le zéro initial ("1500" pour 01500)
        lpad({{ texte_ou_null('code_postal') }}, 5, '0')         as code_postal,
        {{ texte_ou_null('direction_regionale_gares') }}         as direction_regionale,
        {{ texte_ou_null('segmentation_drg') }}                  as segmentation_drg,
        {{ texte_ou_null('segmentation_marketing') }}            as segmentation_marketing,
        round({{ vers_double('non_voyageurs') }}, 4)             as taux_non_voyageurs,
        source_fichier,
        date_ingestion
    from source

)

select
    nettoyage.code_uic,
    nom_gare,
    code_postal,
    -- Département déduit du code postal (approximation : quelques communes
    -- ont un code postal rattaché à un département voisin).
    case
        when code_postal like '97%' then substr(code_postal, 1, 3)
        when code_postal like '20%' then
            case when code_postal < '20200' then '2A' else '2B' end
        else substr(code_postal, 1, 2)
    end                                                          as code_departement,
    direction_regionale,
    segmentation_drg,
    segmentation_marketing,
    taux_non_voyageurs,
    -- Position GPS (référentiel « Gares de voyageurs ») : NULL pour les
    -- gares absentes de ce référentiel (~8 %).
    geo.latitude,
    geo.longitude,
    geo.code_commune,
    source_fichier,
    date_ingestion
from nettoyage
left join geolocalisation as geo
    on geo.code_uic = nettoyage.code_uic
