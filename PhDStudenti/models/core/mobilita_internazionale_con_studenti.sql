{{ config(
    materialized = 'table'
) }}

with src as (
    select
        matricola as matricola_raw, -- known to be empty sometimes
        cognome,
        nome,
        tutore,
        ciclo,
        tipo,
        paese,
        ente,
        periodo,
        durata_giorni,
        anno,
        data_autorizzazione,
        data_pagamento,
        loaded_at,
        source_file
    from {{ source('staging', 'mobilita_internazionale_con_studenti') }}
),

clean as (
    select
        nullif(trim(matricola_raw), '') as matricola_raw,

        nullif(trim(cognome), '') as cognome,
        nullif(trim(nome), '')    as nome,
        nullif(trim(tutore), '')  as tutore,
        nullif(trim(ciclo), '')   as ciclo,
        nullif(trim(tipo), '')    as tipo,
        nullif(trim(paese), '')   as paese,
        nullif(trim(ente), '')    as ente,
        nullif(trim(periodo), '') as periodo,

        /* durata_giorni: integer */
        case
            when nullif(trim(durata_giorni), '') ~ '^\d+$' then cast(trim(durata_giorni) as integer)
            else null
        end as durata_giorni,

        /* anno: 4-digit year */
        case
            when nullif(trim(anno), '') ~ '^\d{4}$' then cast(trim(anno) as integer)
            else null
        end as anno,

        /* dates: DD/MM/YYYY */
        case
            when nullif(trim(data_autorizzazione), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then to_date(trim(data_autorizzazione), 'DD/MM/YYYY')
            else null
        end as data_autorizzazione,

        case
            when nullif(trim(data_pagamento), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                then to_date(trim(data_pagamento), 'DD/MM/YYYY')
            else null
        end as data_pagamento,

        /* normalized join keys (lower + trim + collapse spaces) */
        lower(regexp_replace(coalesce(trim(cognome), ''), '\s+', ' ', 'g')) as cognome_norm,
        lower(regexp_replace(coalesce(trim(nome), ''),    '\s+', ' ', 'g')) as nome_norm,

        loaded_at,
        source_file
    from src
),

students_norm as (
    select
        matricola_dottorando,
        lower(regexp_replace(coalesce(trim(cognome), ''), '\s+', ' ', 'g')) as cognome_norm,
        lower(regexp_replace(coalesce(trim(nome), ''),    '\s+', ' ', 'g')) as nome_norm
    from {{ ref('students') }}
    where cognome is not null
      and nome is not null
      and matricola_dottorando is not null
),

student_name_counts as (
    /* detect ambiguous names */
    select
        cognome_norm,
        nome_norm,
        count(*) as cnt
    from students_norm
    group by 1,2
),

joined as (
    select
        c.*,

        /* Prefer matricola if present; else fill from unique (cognome,nome) match */
        case
            when c.matricola_raw is not null then c.matricola_raw
            when snc.cnt = 1 then s.matricola_dottorando
            else null
        end as matricola,

        snc.cnt as matched_students_count
    from clean c
    left join student_name_counts snc
        on c.cognome_norm = snc.cognome_norm
       and c.nome_norm = snc.nome_norm
    left join students_norm s
        on c.cognome_norm = s.cognome_norm
       and c.nome_norm = s.nome_norm
),

with_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'matricola',
            'cognome',
            'nome',
            'ciclo',
            'tipo',
            'paese',
            'ente',
            'periodo',
            'durata_giorni',
            'anno',
            'data_autorizzazione',
            'data_pagamento'
        ]) }} as id,

        matricola, -- FK -> students.matricola_dottorando (once extracted)
        cognome,
        nome,
        tutore,
        ciclo,
        tipo,
        paese,
        ente,
        periodo,
        durata_giorni,
        anno,
        data_autorizzazione,
        data_pagamento,

        matched_students_count,
        loaded_at,
        source_file
    from joined
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
    cognome,
    nome,
    tutore,
    ciclo,
    tipo,
    paese,
    ente,
    periodo,
    durata_giorni,
    anno,
    data_autorizzazione,
    data_pagamento,
    matched_students_count
from dedup
