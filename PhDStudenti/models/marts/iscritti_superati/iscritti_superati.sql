{{ config(materialized='table') }}

select *
FROM core.filtered_iu_stats