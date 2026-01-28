{{ config(materialized='table') }}

select 
  tb2.matricola_dottorando::text as matricola, tb1.cognome, tb1.nome, tb1.ciclo, tb1.numero_journal as "Journal", 
tb1.numero_conferenze as "Conferenze", 
tb1.numero_capitoli as "Capitoli", 
tb1.numero_poster as "Poster", 
tb1.numero_abstract as "Abstract", 
tb1.numero_brevetti as "Brevetti"
from core.stat_pubb tb1
left join core.students tb2 on tb1.cognome = tb2.cognome and tb1.nome = tb2.nome
ORDER BY (COALESCE(numero_journal,0) 
  + COALESCE(numero_conferenze,0)
  + COALESCE(numero_capitoli,0)
  + COALESCE(numero_poster,0)
  + COALESCE(numero_abstract,0)
  + COALESCE(numero_brevetti,0)
  ) Desc