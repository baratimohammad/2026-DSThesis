{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        cognome,
        nome,
        numero_journal,
        numero_conferenze,
        numero_capitoli,
        numero_poster,
        numero_abstract,
        numero_brevetti,
        loaded_at,
        source_file
    from {{ source('staging', 'stat_pubb') }}
),

clean as (
    select
        nullif(trim(matricola), '') as matricola,
        nullif(trim(ciclo), '')     as ciclo,
        nullif(trim(cognome), '')   as cognome,
        nullif(trim(nome), '')      as nome,

        case when nullif(trim(numero_journal), '') ~ '^\d+$' then cast(trim(numero_journal) as integer) else null end as numero_journal,
        case when nullif(trim(numero_conferenze), '') ~ '^\d+$' then cast(trim(numero_conferenze) as integer) else null end as numero_conferenze,
        case when nullif(trim(numero_capitoli), '') ~ '^\d+$' then cast(trim(numero_capitoli) as integer) else null end as numero_capitoli,
        case when nullif(trim(numero_poster), '') ~ '^\d+$' then cast(trim(numero_poster) as integer) else null end as numero_poster,
        case when nullif(trim(numero_abstract), '') ~ '^\d+$' then cast(trim(numero_abstract) as integer) else null end as numero_abstract,
        case when nullif(trim(numero_brevetti), '') ~ '^\d+$' then cast(trim(numero_brevetti) as integer) else null end as numero_brevetti,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'numero_journal',
            'numero_conferenze',
            'numero_capitoli',
            'numero_poster',
            'numero_abstract',
            'numero_brevetti'
        ]) }} as id,

        matricola, -- FK -> students.matricola_dipendente_docente (per spec)
        ciclo,
        cognome,
        nome,
        numero_journal,
        numero_conferenze,
        numero_capitoli,
        numero_poster,
        numero_abstract,
        numero_brevetti,

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
    matricola,
    ciclo,
    cognome,
    nome,
    numero_journal,
    numero_conferenze,
    numero_capitoli,
    numero_poster,
    numero_abstract,
    numero_brevetti
from dedup
