{{ config(materialized='table') }}

WITH reformatted AS (
  SELECT
    matricola,
    ciclo,
    EXTRACT(YEAR FROM data_esame) AS anno_esame,
    nome_insegnamento,
    CASE WHEN tipo_form = 'Hard' THEN ore_riconosciute ELSE 0 END AS ore_hard_skills,
    CASE WHEN tipo_form = 'Soft' THEN ore_riconosciute ELSE 0 END AS ore_soft_skills
  FROM core.attivita_interne
  WHERE punti > 0
),
aggregated AS (
  SELECT
    matricola,
    ciclo,
    anno_esame AS anno,
    SUM(ore_hard_skills) AS ore_hard,
    SUM(ore_soft_skills) AS ore_soft,
    STRING_AGG(
      'TIPO: ' ||
      CASE
        WHEN ore_hard_skills <> 0 THEN 'Hard'
        WHEN ore_soft_skills <> 0 THEN 'Soft'
        ELSE 'N/A'
      END ||
      ', CORSO: ' || nome_insegnamento ||
      ', ORE: ' ||
      CASE
        WHEN ore_hard_skills <> 0 THEN ore_hard_skills::text
        WHEN ore_soft_skills <> 0 THEN ore_soft_skills::text
        ELSE '0'
      END,
      E'\n\n'
      ORDER BY nome_insegnamento
    ) AS corsi_ore
  FROM reformatted
  GROUP BY matricola, ciclo, anno_esame
),
finalized AS (
  SELECT
    ag.matricola,
    st.cognome,
    st.nome,
    ag.ciclo::text AS ciclo,
    ag.anno,
    SUM(ag.ore_hard) AS ore_hard,
    SUM(ag.ore_soft) AS ore_soft,
    STRING_AGG(ag.corsi_ore, E'\n' ORDER BY ag.anno) AS corsi_ore
  FROM aggregated ag
  LEFT JOIN core.students st
    ON ag.matricola = st.matricola_dottorando
  GROUP BY ag.matricola, st.cognome, st.nome, ag.ciclo, ag.anno
)
SELECT *
FROM finalized
ORDER BY (COALESCE(ore_hard,0) + COALESCE(ore_soft,0)) DESC
