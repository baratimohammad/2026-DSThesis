{{ config(materialized='table') }}

with src as(SELECT st.matricola_dottorando as matricola,
 st.cognome, st.nome, sp.titolo, st.ciclo, pb.anno
FROM core.students st
left join core.student_publications sp on st.matricola_dottorando = sp.matricola
left join core.publications pb on sp.titolo = pb.titolo
where pb.titolo <> '')

select matricola, cognome, nome, ciclo, anno, count(*) as publication_count, STRING_AGG(titolo, '; ') as titles
FROM src 
group by matricola, cognome, nome, ciclo, anno
order by count(*) desc
