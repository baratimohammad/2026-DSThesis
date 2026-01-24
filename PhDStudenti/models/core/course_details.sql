{{ config(
    materialized = 'table'
) }}

with src as (
    select
        cod_ins,
        anno,
        type,
        teacher,
        status,
        ssd,
        h_les,
        h_ex,
        h_lab,
        h_tut,
        years_teaching,
        loaded_at
    from {{ source('staging', 'dettaglio_corso') }}
),

clean as (
    select
        nullif(trim(cod_ins), '') as cod_ins,

        case
            when nullif(trim(anno), '') ~ '^\d{4}$' then cast(trim(anno) as integer)
            else null
        end as anno,

        nullif(trim(type), '')    as type,
        nullif(trim(teacher), '') as teacher,
        nullif(trim(status), '')  as status,
        nullif(trim(ssd), '')     as ssd,

        /* hours: allow decimals and commas */
        cast(nullif(replace(trim(h_les), ',', '.'), '') as numeric(10,2)) as h_les,
        cast(nullif(replace(trim(h_ex),  ',', '.'), '') as numeric(10,2)) as h_ex,
        cast(nullif(replace(trim(h_lab), ',', '.'), '') as numeric(10,2)) as h_lab,
        cast(nullif(replace(trim(h_tut), ',', '.'), '') as numeric(10,2)) as h_tut,

        /* years_teaching: should be integer-ish */
        case
            when nullif(trim(years_teaching), '') ~ '^\d+$' then cast(trim(years_teaching) as integer)
            else null
        end as years_teaching,

        loaded_at
    from src
),

dedup as (
    /*
      PK: (cod_ins, anno)
      If duplicates arrive, keep latest by loaded_at.
    */
    select *
    from (
        select
            c.*,
            row_number() over (
                partition by c.cod_ins, c.anno
                order by c.loaded_at desc
            ) as rn
        from clean c
        where c.cod_ins is not null
          and c.anno is not null
    ) x
    where rn = 1
)

select
    cod_ins,
    anno,
    type,
    teacher,
    status,
    ssd,
    h_les,
    h_ex,
    h_lab,
    h_tut,
    years_teaching
from dedup