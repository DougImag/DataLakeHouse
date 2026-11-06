{#
  Position GPS des gares de voyageurs : une ligne par code UIC.

  - position_geographique "<lat>, <lon>" est découpée en latitude / longitude
    (latitude EN PREMIER dans la source) ;
  - une gare peut avoir plusieurs codes UIC ("87271494;87001479") : elle
    produit une ligne par code, pour permettre la jointure avec slv_gares ;
  - seule la dernière ingestion Bronze est conservée.
#}

with source as (

    select *
    from {{ source('bronze', 'gares_voyageurs') }}
    where date_ingestion = (
        select max(date_ingestion) from {{ source('bronze', 'gares_voyageurs') }}
    )

),

nettoyage as (

    select
        {{ texte_ou_null('id') }}                                     as id_gare,
        {{ texte_ou_null('nom') }}                                    as nom_gare,
        {{ texte_ou_null('libellecourt') }}                           as trigramme,
        try_cast(trim(split_part(position_geographique, ',', 1)) as double) as latitude,
        try_cast(trim(split_part(position_geographique, ',', 2)) as double) as longitude,
        {{ texte_ou_null('codeinsee') }}                              as code_commune,
        -- Gare multi-segments ("A;B") : on garde le plus élevé (A > B > C)
        array_min(split({{ texte_ou_null('segment_drg') }}, ';'))     as segment_drg,
        split({{ texte_ou_null('codes_uic') }}, ';')                  as codes_uic,
        source_fichier,
        date_ingestion
    from source

)

select
    trim(code_uic)  as code_uic,
    id_gare,
    nom_gare,
    trigramme,
    latitude,
    longitude,
    code_commune,
    segment_drg,
    source_fichier,
    date_ingestion
from nettoyage
cross join unnest(codes_uic) as u (code_uic)
where trim(code_uic) <> ''
