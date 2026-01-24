{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        denominazione,
        ore_dichiarate,
        ore_riconosciute,
        ore_calcolate,
        coeff_voto,
        punti,
        tipo_form,
        tipo_richiesta,
        liv_esame,
        data_attivita,
        data_convalida,
        loaded_at,
        source_file
    from {{ source('staging', 'attivita_formative_esterne') }}
),

clean as (
    select
        nullif(trim(matricola), '')      as matricola,
        nullif(trim(ciclo), '')          as ciclo,
        nullif(trim(denominazione), '')  as denominazione,

        /* hours + points: allow decimals and commas */
        cast(nullif(replace(trim(ore_dichiarate), ',', '.'), '')   as numeric(10,2)) as ore_dichiarate,
        cast(nullif(replace(trim(ore_riconosciute), ',', '.'), '') as numeric(10,2)) as ore_riconosciute,
        cast(nullif(replace(trim(ore_calcolate), ',', '.'), '')    as numeric(10,2)) as ore_calcolate,

        cast(nullif(replace(trim(coeff_voto), ',', '.'), '') as numeric(8,4))  as coeff_voto,
        cast(nullif(replace(trim(punti), ',', '.'), '')      as numeric(12,2)) as punti,

        nullif(trim(tipo_form), '')      as tipo_form,
        nullif(trim(tipo_richiesta), '') as tipo_richiesta,
        nullif(trim(liv_esame), '')      as liv_esame,

        /* Dates: your real format is DD/MM/YYYY (also works with D/M/YYYY) */
        case
            when nullif(trim(data_attivita), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then to_date(trim(data_attivita), 'DD/MM/YYYY')
            else null
        end as data_attivita,

        case
            when nullif(trim(data_convalida), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then to_date(trim(data_convalida), 'DD/MM/YYYY')
            else null
        end as data_convalida,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'denominazione',
            'tipo_form',
            'tipo_richiesta',
            'liv_esame',
            'data_attivita',
            'data_convalida',
            'ore_dichiarate',
            'ore_riconosciute',
            'ore_calcolate',
            'coeff_voto',
            'punti'
        ]) }} as id,

        matricola,     -- FK -> students.matricola_dottorando
        ciclo,
        denominazione,
        ore_dichiarate,
        ore_riconosciute,
        ore_calcolate,
        coeff_voto,
        punti,
        tipo_form,
        tipo_richiesta,
        liv_esame,
        data_attivita,
        data_convalida,

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
    denominazione,
    ore_dichiarate,
    ore_riconosciute,
    ore_calcolate,
    coeff_voto,
    punti,
    tipo_form,
    tipo_richiesta,
    liv_esame,
    data_attivita,
    data_convalida
from dedup;
