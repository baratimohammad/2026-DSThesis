{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola_dott,
        cognome,
        nome,
        ciclo,
        tutor,
        ore,
        tipo_attivita,
        materia,
        docente,
        corso_di_laurea,
        loaded_at,
        source_file
    from {{ source('staging', 'collaborazioni_dettaglio') }}
),

clean as (
    select
        nullif(trim(matricola_dott), '') as matricola,
        nullif(trim(cognome), '')        as cognome,
        nullif(trim(nome), '')           as nome,
        nullif(trim(ciclo), '')          as ciclo,
        nullif(trim(tutor), '')          as tutor,

        cast(nullif(replace(trim(ore), ',', '.'), '') as numeric(10,2)) as ore,

        nullif(trim(tipo_attivita), '')  as tipo_attivita,
        nullif(trim(materia), '')        as materia,
        nullif(trim(docente), '')        as docente,
        nullif(trim(corso_di_laurea), '') as corso_di_laurea,

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
            'tipo_attivita',
            'materia',
            'docente',
            'corso_di_laurea',
            'ore'
        ]) }} as id,

        matricola,  -- FK -> students.matricola_dottorando
        ciclo,
        cognome,
        nome,
        tutor,
        ore,
        tipo_attivita,
        materia,
        docente,
        corso_di_laurea,

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
    ore,
    tipo_attivita,
    materia,
    docente,
    corso_di_laurea
from dedup;
