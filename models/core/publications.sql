version: 2

models:
  - name: publications
    description: "Core publications dimension (one row per titolo)."
    columns:
      - name: titolo
        tests:
          - not_null
          - unique

      - name: autore_cognome
        tests:
          - not_null:
              config:
                severity: warn

      - name: autore_nome
        tests:
          - not_null:
              config:
                severity: warn

      - name: anno
        tests:
          - dbt_utils.expression_is_true:
              expression: "anno is null or (anno between 1900 and extract(year from current_date)::int + 1)"
              config:
                severity: warn
