{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        titolo,
        grado_proprieta_dottorandi,
        punteggio,
        grado_proprieta,
        loaded_at
    from {{ source('staging', 'pubblicazioni') }}
),

clean as (
    select
        nullif(trim(matricola), '') as matricola,
        nullif(trim(titolo), '')    as titolo,

        /* numeric-ish fields: tolerate commas and blanks */
        cast(nullif(replace(trim(grado_proprieta_dottorandi), ',', '.'), '') as numeric(6,2))
            as grado_proprieta_dottorandi,

        cast(nullif(replace(trim(punteggio), ',', '.'), '') as numeric(12,2))
            as punteggio,

        cast(nullif(replace(trim(grado_proprieta), ',', '.'), '') as numeric(6,2))
            as grado_proprieta,

        loaded_at
    from src
),

dedup as (
    /*
      Compound PK: (matricola, titolo)
      If the same pair is loaded multiple times, keep the latest by loaded_at.
    */
    select *
    from (
        select
            c.*,
            row_number() over (
                partition by c.matricola, c.titolo
                order by c.loaded_at desc
            ) as rn
        from clean c
        where c.matricola is not null
          and c.titolo is not null
    ) x
    where rn = 1
)

select
    matricola,                 -- FK -> students.matricola_dottorando
    titolo,                    -- FK -> publications.titolo
    grado_proprieta_dottorandi,
    punteggio,
    grado_proprieta
from dedup
