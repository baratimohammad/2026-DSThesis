{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        cognome,
        nome,
        tutor,
        ore_soft_skill,
        ore_hard_skill,
        ore_totali,
        loaded_at,
        source_file
    from {{ source('staging', 'ore_formazione') }}
),

clean as (
    select
        nullif(trim(matricola), '') as matricola,
        nullif(trim(ciclo), '')     as ciclo,
        nullif(trim(cognome), '')   as cognome,
        nullif(trim(nome), '')      as nome,
        nullif(trim(tutor), '')     as tutor,

        cast(nullif(replace(trim(ore_soft_skill), ',', '.'), '') as numeric(10,2)) as ore_soft_skill,
        cast(nullif(replace(trim(ore_hard_skill), ',', '.'), '') as numeric(10,2)) as ore_hard_skill,
        cast(nullif(replace(trim(ore_totali), ',', '.'), '')     as numeric(10,2)) as ore_totali,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'tutor',
            'ore_soft_skill',
            'ore_hard_skill',
            'ore_totali'
        ]) }} as id,

        matricola,  -- FK -> students.matricola_dottorando
        ciclo,
        cognome,
        nome,
        tutor,
        ore_soft_skill,
        ore_hard_skill,
        ore_totali,

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
    tutor,
    ore_soft_skill,
    ore_hard_skill,
    ore_totali
from dedup;
