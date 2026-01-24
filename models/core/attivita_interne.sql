{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        cod_ins,
        nome_insegnamento,
        ore,
        ore_riconosciute,
        voto,
        coeff_voto,
        data_esame,
        tipo_form,
        liv_esame,
        tipo_attivita,
        punti,
        loaded_at,
        source_file
    from {{ source('staging', 'attivita_formative_interne') }}
),

clean as (
    select
        nullif(trim(matricola), '') as matricola,
        nullif(trim(ciclo), '')     as ciclo,
        nullif(trim(cod_ins), '')   as cod_ins,
        nullif(trim(nome_insegnamento), '') as nome_insegnamento,

        cast(nullif(replace(trim(ore), ',', '.'), '')            as numeric(10,2)) as ore,
        cast(nullif(replace(trim(ore_riconosciute), ',', '.'), '') as numeric(10,2)) as ore_riconosciute,

        /* voto sometimes numeric, sometimes text; keep as text but add coeff/punti numeric */
        nullif(trim(voto), '') as voto,
        cast(nullif(replace(trim(coeff_voto), ',', '.'), '') as numeric(8,4))  as coeff_voto,
        cast(nullif(replace(trim(punti), ',', '.'), '')      as numeric(12,2)) as punti,

        case
            when nullif(trim(data_esame), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then to_date(trim(data_esame), 'DD/MM/YYYY')
            else null
        end as data_esame,

        /* derive anno for the relationship to students_courses */
        case
            when nullif(trim(data_esame), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then extract(year from to_date(trim(data_esame), 'DD/MM/YYYY'))::int
            else null
        end as anno,

        nullif(trim(tipo_form), '')     as tipo_form,
        nullif(trim(liv_esame), '')     as liv_esame,
        nullif(trim(tipo_attivita), '') as tipo_attivita,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'cod_ins',
            'nome_insegnamento',
            'data_esame',
            'tipo_form',
            'liv_esame',
            'tipo_attivita',
            'ore',
            'ore_riconosciute',
            'voto',
            'coeff_voto',
            'punti'
        ]) }} as id,

        matricola, -- FK -> students.matricola_dottorando
        ciclo,
        cod_ins,
        anno,      -- derived
        nome_insegnamento,
        ore,
        ore_riconosciute,
        voto,
        coeff_voto,
        data_esame,
        tipo_form,
        liv_esame,
        tipo_attivita,
        punti,

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
    cod_ins,
    anno,
    nome_insegnamento,
    ore,
    ore_riconosciute,
    voto,
    coeff_voto,
    data_esame,
    tipo_form,
    liv_esame,
    tipo_attivita,
    punti
from dedup;
