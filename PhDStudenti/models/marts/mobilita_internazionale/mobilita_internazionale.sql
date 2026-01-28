{{ config(materialized='table') }}

with src as (
  select distinct matricola, cognome, nome, tutore, ciclo, tipo, paese, ente, durata_giorni, anno from core.mobilita_internazionale_con_studenti
)

SELECT matricola, cognome, nome, durata_giorni, ciclo, anno, paese, tipo , ente 
FROM src
order by durata_giorni DESC