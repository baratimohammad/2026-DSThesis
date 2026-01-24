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

-- dedup as (
--     /*
--       PK: (cod_ins, anno, matricola)
--       If duplicates arrive, keep latest by loaded_at.
--     */
--     select *
--     from (
--         select
--             c.*,
--             row_number() over (
--                 partition by c.cod_ins, c.anno, c.matricola
--                 order by c.loaded_at desc
--             ) as rn
--         from clean c
--         where c.cod_ins is not null
--           and c.anno is not null
--           and c.matricola is not null
--     ) x
--     where rn = 1
-- )

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