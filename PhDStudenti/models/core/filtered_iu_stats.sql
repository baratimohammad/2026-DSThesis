{{ config(
    materialized = 'table'
) }}

with src as (
    select
        cod_ins,
        nome_insegnamento,
        docente,
        iscritti_2025,
        superati_2025,
        iis_iscritti_2025,
        iis_superati_2025,
        iscritti_2024,
        superati_2024,
        iis_iscritti_2024,
        iis_superati_2024,
        iscritti_2023,
        superati_2023,
        iis_iscritti_2023,
        iis_superati_2023,
        iscritti_2022,
        superati_2022,
        iis_iscritti_2022,
        iis_superati_2022,
        loaded_at,
        source_file
    from {{ source('staging', 'filtered_iu_stats') }}
),

clean as (
    select
        nullif(trim(cod_ins), '')          as cod_ins,
        nullif(trim(nome_insegnamento), '') as nome_insegnamento,
        nullif(trim(docente), '')          as docente,

        /* counts: strict integers only */
        case when nullif(trim(iscritti_2025), '') ~ '^\d+$' then cast(trim(iscritti_2025) as integer) else null end as iscritti_2025,
        case when nullif(trim(superati_2025), '') ~ '^\d+$' then cast(trim(superati_2025) as integer) else null end as superati_2025,
        case when nullif(trim(iis_iscritti_2025), '') ~ '^\d+$' then cast(trim(iis_iscritti_2025) as integer) else null end as iis_iscritti_2025,
        case when nullif(trim(iis_superati_2025), '') ~ '^\d+$' then cast(trim(iis_superati_2025) as integer) else null end as iis_superati_2025,

        case when nullif(trim(iscritti_2024), '') ~ '^\d+$' then cast(trim(iscritti_2024) as integer) else null end as iscritti_2024,
        case when nullif(trim(superati_2024), '') ~ '^\d+$' then cast(trim(superati_2024) as integer) else null end as superati_2024,
        case when nullif(trim(iis_iscritti_2024), '') ~ '^\d+$' then cast(trim(iis_iscritti_2024) as integer) else null end as iis_iscritti_2024,
        case when nullif(trim(iis_superati_2024), '') ~ '^\d+$' then cast(trim(iis_superati_2024) as integer) else null end as iis_superati_2024,

        case when nullif(trim(iscritti_2023), '') ~ '^\d+$' then cast(trim(iscritti_2023) as integer) else null end as iscritti_2023,
        case when nullif(trim(superati_2023), '') ~ '^\d+$' then cast(trim(superati_2023) as integer) else null end as superati_2023,
        case when nullif(trim(iis_iscritti_2023), '') ~ '^\d+$' then cast(trim(iis_iscritti_2023) as integer) else null end as iis_iscritti_2023,
        case when nullif(trim(iis_superati_2023), '') ~ '^\d+$' then cast(trim(iis_superati_2023) as integer) else null end as iis_superati_2023,

        case when nullif(trim(iscritti_2022), '') ~ '^\d+$' then cast(trim(iscritti_2022) as integer) else null end as iscritti_2022,
        case when nullif(trim(superati_2022), '') ~ '^\d+$' then cast(trim(superati_2022) as integer) else null end as superati_2022,
        case when nullif(trim(iis_iscritti_2022), '') ~ '^\d+$' then cast(trim(iis_iscritti_2022) as integer) else null end as iis_iscritti_2022,
        case when nullif(trim(iis_superati_2022), '') ~ '^\d+$' then cast(trim(iis_superati_2022) as integer) else null end as iis_superati_2022,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cod_ins',
            'nome_insegnamento',
            'docente'
        ]) }} as id,

        cod_ins, -- FK -> course_details.cod_ins (loose FK)
        nome_insegnamento,
        docente,
        iscritti_2025,
        superati_2025,
        iis_iscritti_2025,
        iis_superati_2025,
        iscritti_2024,
        superati_2024,
        iis_iscritti_2024,
        iis_superati_2024,
        iscritti_2023,
        superati_2023,
        iis_iscritti_2023,
        iis_superati_2023,
        iscritti_2022,
        superati_2022,
        iis_iscritti_2022,
        iis_superati_2022,

        loaded_at,
        source_file
    from clean
),

dedup as (
    select *
    from (
        select
            w.*,
            row_number() over (
                partition by w.id
                order by w.loaded_at desc
            ) as rn
        from with_id w
        where w.id is not null
    ) x
    where rn = 1
)

select
    id,
    cod_ins,
    nome_insegnamento,
    docente,
    iscritti_2025,
    superati_2025,
    iis_iscritti_2025,
    iis_superati_2025,
    iscritti_2024,
    superati_2024,
    iis_iscritti_2024,
    iis_superati_2024,
    iscritti_2023,
    superati_2023,
    iis_iscritti_2023,
    iis_superati_2023,
    iscritti_2022,
    superati_2022,
    iis_iscritti_2022,
    iis_superati_2022
from dedup
