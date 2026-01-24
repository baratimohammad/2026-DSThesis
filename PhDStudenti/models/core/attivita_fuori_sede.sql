{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola,
        ciclo,
        denominazione,
        luogo,
        ente,
        periodo,
        data_autorizzazione,
        data_aut_pagamento,
        data_attestaz,
        data_convalida,
        loaded_at,
        source_file
    from {{ source('staging', 'attivita_formative_fuorisede') }}
),

clean as (
    select
        nullif(trim(matricola), '')      as matricola,
        nullif(trim(ciclo), '')          as ciclo,
        nullif(trim(denominazione), '')  as denominazione,
        nullif(trim(luogo), '')          as luogo,
        nullif(trim(ente), '')           as ente,
        nullif(trim(periodo), '')        as periodo,

        /* Dates: best-effort parsing; if formats are garbage, they become null */
        to_date(nullif(trim(data_autorizzazione), ''), 'DD/MM/YYYY') as data_autorizzazione,
        to_date(nullif(trim(data_aut_pagamento), ''),  'DD/MM/YYYY') as data_aut_pagamento,
        to_date(nullif(trim(data_attestaz), ''),       'DD/MM/YYYY') as data_attestaz,
        to_date(nullif(trim(data_convalida), ''),      'DD/MM/YYYY') as data_convalida,

        loaded_at,
        source_file
    from src
),

with_id as (
    select
        /*
          Deterministic surrogate key:
          same activity -> same id across runs.
          If your business grain differs, change the fields in this hash.
        */
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'ciclo',
            'denominazione',
            'luogo',
            'ente',
            'periodo',
            'data_autorizzazione',
            'data_aut_pagamento',
            'data_attestaz',
            'data_convalida'
        ]) }} as id,

        matricola,            -- FK -> students.matricola_dottorando
        ciclo,
        denominazione,
        luogo,
        ente,
        periodo,
        data_autorizzazione,
        data_aut_pagamento,
        data_attestaz,
        data_convalida,

        loaded_at,
        source_file
    from clean
),

dedup as (
    /*
      In case identical rows are reloaded, keep the latest.
    */
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
    luogo,
    ente,
    periodo,
    data_autorizzazione,
    data_aut_pagamento,
    data_attestaz,
    data_convalida
from dedup