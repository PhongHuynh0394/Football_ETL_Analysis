{{ config(materialized='table') }}
SELECT 
    name,
    season,
    yellowCards,
    redCards,
    fouls
FROM {{ref('statsPerLeagueSeason')}}
