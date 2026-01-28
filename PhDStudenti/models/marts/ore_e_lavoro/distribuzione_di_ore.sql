{{ config(materialized='table') }}

SELECT
  st.matricola_dottorando as matricola,
  st.nome,
  st.cognome,
  st.ciclo,
  sc.anno,
  COALESCE(SUM(cd.h_les), 0) AS hour_les,
  COALESCE(SUM(cd.h_ex), 0)  AS hour_ex,
  COALESCE(SUM(cd.h_lab), 0) AS hour_lab,
  COALESCE(SUM(cd.h_tut), 0) AS hour_tut,
  STRING_AGG(DISTINCT cd.cod_ins, ', ' ORDER BY cd.cod_ins) AS insegnamenti
FROM core.students st
LEFT JOIN core.students_courses sc
  ON st.matricola_dottorando = sc.matricola
LEFT JOIN core.course_details cd
  ON sc.cod_ins = cd.cod_ins
 AND sc.anno   = cd.anno

GROUP BY st.matricola_dottorando, st.nome, st.cognome, st.ciclo, sc.anno
ORDER BY
  COALESCE(SUM(cd.h_les), 0)
+ COALESCE(SUM(cd.h_ex),  0)
+ COALESCE(SUM(cd.h_lab), 0)
+ COALESCE(SUM(cd.h_tut), 0) DESC