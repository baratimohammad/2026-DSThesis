{{ config(
    materialized = 'table'
) }}

with src as (
    select
        cod_ins,
        anno,
        matricola,
        cognome,
        nome,
        cod_insegnamento_col,
        cod_corso_dottorato,
        periodo_didattico,
        stato,
        loaded_at
    from {{ source('staging', 'corsi') }}
),

clean as (
    select
        nullif(trim(cod_ins), '') as cod_ins,

        case
            when nullif(trim(anno), '') ~ '^\d{4}$' then cast(trim(anno) as integer)
            else null
        end as anno,

        nullif(trim(matricola), '') as matricola,
        nullif(trim(cognome), '')   as cognome,
        nullif(trim(nome), '')      as nome,

        nullif(trim(cod_insegnamento_col), '') as cod_insegnamento_col,
        nullif(trim(cod_corso_dottorato), '')  as cod_corso_dottorato,
        nullif(trim(periodo_didattico), '')    as periodo_didattico,

        nullif(trim(stato), '') as stato,

        loaded_at
    from src
)

select
    cod_ins,
    anno,
    matricola,
    cognome,
    nome,
    cod_insegnamento_col,
    cod_corso_dottorato,
    periodo_didattico,
    stato
from clean