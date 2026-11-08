{{ config(materialized='ephemeral') }}

{#
  Si plusieurs fichiers ont été ingérés en Bronze, on ne garde pour chaque
  gare que la ligne de l'ingestion la plus récente.
#}

with classement as (

    select
        *,
        row_number() over (
            partition by trim(code_uic_complet)
            order by date_ingestion desc, source_fichier desc
        ) as rang_ingestion
    from {{ source('bronze', 'frequentation_sncf_v2') }}
    where {{ texte_ou_null('code_uic_complet') }} is not null

)

select * from classement
where rang_ingestion = 1
