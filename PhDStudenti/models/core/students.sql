{{ config(
    materialized = 'table'
) }}

with src as (
    select
        -- raw fields
        matricola_dottorando,
        matricola_dipendente_docente,
        email,
        cognome,
        nome,
        ciclo,
        tutore,
        co_tutore,
        status,
        ore_soft_skills,
        ore_hard_skills,
        punti_soft_skills,
        punti_hard_skills,
        punti_attivita_fuorisede,
        punti_totali,
        loaded_at
    from {{ source('staging', 'students') }}
),

clean as (
    select
        nullif(trim(matricola_dottorando), '')                         as matricola_dottorando,
        nullif(trim(matricola_dipendente_docente), '')                 as matricola_dipendente_docente,
        nullif(lower(trim(email)), '')                                 as email,
        nullif(trim(cognome), '')                                      as cognome,
        nullif(trim(nome), '')                                         as nome,
        nullif(trim(ciclo), '')                                         as ciclo,
        nullif(trim(tutore), '')                                       as tutore,
        nullif(trim(co_tutore), '')                                    as co_tutore,
        nullif(trim(status), '')                                       as status,

        /* Hours: allow decimals, tolerate commas, empty -> null */
        cast(nullif(replace(trim(ore_soft_skills), ',', '.'), '') as numeric(10,2))  as ore_soft_skills,
        cast(nullif(replace(trim(ore_hard_skills), ',', '.'), '') as numeric(10,2))  as ore_hard_skills,

        /* Points: allow decimals, tolerate commas, empty -> null */
        cast(nullif(replace(trim(punti_soft_skills), ',', '.'), '') as numeric(12,2))         as punti_soft_skills,
        cast(nullif(replace(trim(punti_hard_skills), ',', '.'), '') as numeric(12,2))         as punti_hard_skills,
        cast(nullif(replace(trim(punti_attivita_fuorisede), ',', '.'), '') as numeric(12,2))  as punti_attivita_fuorisede,
        cast(nullif(replace(trim(punti_totali), ',', '.'), '') as numeric(12,2))              as punti_totali,

        loaded_at
    from src
),

dedup as (
    /*
      If the same student appears multiple times across loads/files,
      keep the most recent record (by loaded_at).
      If you DON’T want this behavior, delete this CTE and select from `clean`.
    */
    select *
    from (
        select
            c.*,
            row_number() over (
                partition by c.matricola_dottorando
                order by c.loaded_at desc
            ) as rn
        from clean c
        where c.matricola_dottorando is not null
    ) x
    where rn = 1
)

select
    matricola_dottorando,              -- PK
    matricola_dipendente_docente,
    email,
    cognome,
    nome,
    ciclo,
    tutore,
    co_tutore,
    status,
    ore_soft_skills,
    ore_hard_skills,
    punti_soft_skills,
    punti_hard_skills,
    punti_attivita_fuorisede,
    punti_totali
from dedup