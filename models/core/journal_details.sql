{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        cognome,
        nome,
        titolo,
        rivista,
        issn,
        anno,
        quartile,
        loaded_at,
        source_file
    from {{ source('staging', 'journal_details') }}
),

clean as (
    select
        nullif(trim(matricola), '') as matricola,
        nullif(trim(ciclo), '')     as ciclo,
        nullif(trim(cognome), '')   as cognome,
        nullif(trim(nome), '')      as nome,
        nullif(trim(titolo), '')    as titolo,
        nullif(trim(rivista), '')   as rivista,

        /* ISSN: keep digits + X, normalize hyphen if present */
        case
            when nullif(trim(issn), '') is null then null
            else upper(regexp_replace(trim(issn), '[^0-9Xx]', '', 'g'))
        end as issn_raw,

        case
            when nullif(trim(anno), '') ~ '^\d{4}$' then cast(trim(anno) as integer)
            else null
        end as anno,

        /* quartile often "Q1".."Q4" or "1".."4" */
        case
            when lower(nullif(trim(quartile), '')) ~ '^q?[1-4]$'
                then cast(regexp_replace(lower(trim(quartile)), '[^1-4]', '', 'g') as integer)
            else null
        end as quartile,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'titolo',
            'rivista',
            'issn_raw',
            'anno',
            'quartile'
        ]) }} as id,

        matricola,  -- FK (claimed) -> students.matricola_dipendente_docente
        ciclo,
        cognome,
        nome,
        titolo,
        rivista,

        /* format as ####-#### when 8 chars */
        case
            when issn_raw is null then null
            when length(issn_raw) = 8 then substring(issn_raw, 1, 4) || '-' || substring(issn_raw, 5, 4)
            else issn_raw
        end as issn,

        anno,
        quartile,

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
    titolo,
    rivista,
    issn,
    anno,
    quartile
from dedup;
