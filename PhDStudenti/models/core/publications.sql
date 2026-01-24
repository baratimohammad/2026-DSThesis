{{ config(
    materialized = 'table'
) }}

with src as (
    select
        titolo,
        anno,
        tipo,
        rivista,
        autori,
        convegno,
        referee,
        indicatore_r,
        errore_val,
        loaded_at
    from {{ source('staging', 'pubblicazioni') }}
),

clean as (
    select
        /* PK */
        nullif(trim(titolo), '') as titolo,

        /* Year */
        case
            when nullif(trim(anno), '') ~ '^\d{4}$'
                then cast(trim(anno) as integer)
            else null
        end as anno,

        nullif(trim(tipo), '')     as tipo,
        nullif(trim(rivista), '')  as rivista,
        nullif(trim(autori), '')   as autori,
        nullif(trim(convegno), '') as convegno,
        nullif(trim(referee), '') as referee,
        case
            when trim(indicatore_r) ~ '^-?\d+([.,]\d+)?$'
                then cast(replace(trim(indicatore_r), ',', '.') as numeric(12,4))
            else null
            end as indicatore_r,

        case
            when trim(errore_val) ~ '^-?\d+([.,]\d+)?$'
                then cast(replace(trim(errore_val), ',', '.') as numeric(12,4))
            else null
            end as errore_val,


        loaded_at
    from src
),

authors_parsed as (
    select
        *,
        /*
          Split autori by ", "
          Position 1 -> cognome
          Position 2 -> nome
          Anything else is ignored here (multiple authors are NOT normalized yet)
        */
        split_part(autori, ', ', 1) as autore_cognome,
        split_part(autori, ', ', 2) as autore_nome
    from clean
),

dedup as (
    select *
    from (
        select
            a.*,
            row_number() over (
                partition by a.titolo
                order by a.loaded_at desc
            ) as rn
        from authors_parsed a
        where a.titolo is not null
    ) x
    where rn = 1
)

select
    titolo,              -- PK
    anno,
    tipo,
    rivista,
    autori,
    autore_cognome,
    autore_nome,
    convegno,
    referee,
    indicatore_r,
    errore_val
from dedup