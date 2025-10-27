
-------------------------------------------------------------------------------------
-- preprocessing/setting up certain columns before final selection
-------------------------------------------------------------------------------------
-- get school_id from person table
WITH trip_base AS (
    SELECT 
        t.*,
        p.school_type
    FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS t
    LEFT JOIN `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_person` AS p
    ON t.person_id = p.person_id
),

-- calculate 'oCO_TAZID_USTMv3' origin co_tazid
origin_taz_v3 AS (
  SELECT
    t.trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS oCO_TAZID_USTMv3
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v3_taz_2021_09_22_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.trip_id
),

-- calculate 'dCO_TAZID_USTMv3' destination co_tazid
destination_taz_v3 AS (
  SELECT
    t.trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS dCO_TAZID_USTMv3
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v3_taz_2021_09_22_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.trip_id
),

origin_taz_v4 AS (
  SELECT
    t.trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS oCO_TAZID_USTMv4,
    SAFE_CAST(ARRAY_AGG(taz.SUBAREAID LIMIT 1)[OFFSET(0)] AS INT64) AS oSUBAREAID
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v4_taz_2025_07_29_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.trip_id
),

-- calculate 'dCO_TAZID_USTMv4' destination co_tazid
destination_taz_v4 AS (
  SELECT
    t.trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS dCO_TAZID_USTMv4,
    SAFE_CAST(ARRAY_AGG(taz.SUBAREAID LIMIT 1)[OFFSET(0)] AS INT64) AS dSUBAREAID
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v4_taz_2025_07_29_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.trip_id
),

-- calculate 'PA_AP' field using o/d purposes 
trip_taz_pa AS (
  SELECT
    tb.*,
    ot3.oCO_TAZID_USTMv3,
    dt3.dCO_TAZID_USTMv3,
    ot4.oCO_TAZID_USTMv4,
    dt4.dCO_TAZID_USTMv4,
    dt4.dSUBAREAID,
    ot4.oSUBAREAID,

    CASE 
      WHEN tb.o_purpose_category = 1 THEN 'PA'
      WHEN tb.d_purpose_category = 1 THEN 'AP'
      WHEN tb.o_purpose_category = 2 AND 
           CASE 
             WHEN tb.d_purpose_category = 1 THEN 'Home'
             WHEN tb.d_purpose_category = 2 THEN 'Work'
             WHEN tb.d_purpose_category IN (995, -1) THEN 'Undefined'
             ELSE 'Other'
           END = 'Other' THEN 'PA'
      WHEN CASE 
             WHEN tb.o_purpose_category = 1 THEN 'Home'
             WHEN tb.o_purpose_category = 2 THEN 'Work'
             WHEN tb.o_purpose_category IN (995, -1) THEN 'Undefined'
             ELSE 'Other'
           END = 'Other' 
           AND tb.d_purpose_category = 2 THEN 'AP'
      WHEN tb.o_purpose_type = 995 OR tb.d_purpose_type = 995 THEN 'Undefined'
      ELSE 'PA'
    END AS PA_AP

  FROM trip_base tb
  LEFT JOIN origin_taz_v3 AS ot3 ON tb.trip_id = ot3.trip_id
  LEFT JOIN destination_taz_v3 AS dt3 ON tb.trip_id = dt3.trip_id
  LEFT JOIN origin_taz_v4 AS ot4 ON tb.trip_id = ot4.trip_id
  LEFT JOIN destination_taz_v4 AS dt4 ON tb.trip_id = dt4.trip_id
), 

-- calculate 'PURP7_Sch' field to better understand school purpose
trip_taz_pa_sch AS (
    SELECT
        t.*,

    CASE
        WHEN t.trip_type = 2 AND (t.o_purpose = 22 OR t.d_purpose = 22) THEN 7
        WHEN t.trip_type = 2 AND (t.o_purpose = 21 OR t.d_purpose = 21) THEN 2
        WHEN t.trip_type = 2 AND (t.o_purpose IN (23,24,25,26) OR t.d_purpose IN (23,24,25,26)) THEN 4
        WHEN t.trip_type = 2 AND ((t.o_purpose = 60 OR t.d_purpose = 60) AND t.school_type IN (11,12,13)) THEN 7
        WHEN t.trip_type = 2 AND ((t.o_purpose = 60 OR t.d_purpose = 60) AND t.school_type IN (5,6,7)) THEN 2
        WHEN t.trip_type = 2 THEN 4
        ELSE NULL
    END AS PURP7_Sch

    FROM trip_taz_pa t
),

-- calculate 'PURP7' using 'PURP7_Sch' to divide purpose into 7 categories
trip_taz_pa_sch_purp AS (
  SELECT
    t.*,

    CASE
      WHEN t.trip_type = 1 THEN 1
      WHEN t.trip_type = 2 AND t.PURP7_Sch = 2 THEN 2
      WHEN t.trip_type = 2 AND t.PURP7_Sch = 4 THEN 4
      WHEN t.trip_type = 2 AND t.PURP7_Sch = 7 THEN 7
      WHEN t.trip_type = 3 THEN 3
      WHEN t.trip_type = 4 THEN 4
      WHEN t.trip_type = 5 THEN 4
      WHEN t.trip_type = 6 THEN 5
      WHEN t.trip_type = 7 THEN 6
      WHEN t.trip_type = 995 THEN 995
      ELSE 10
    END AS PURP7
  FROM trip_taz_pa_sch t
)


-------------------------------------------------------------------------------------
-- calculate remaining trip fields using preprocessed tables from above
-------------------------------------------------------------------------------------
SELECT 
  t.* EXCEPT(Unnamed__0, segment_type,
  o_taz,o_lon,o_lat,o_x,o_y,
  d_taz,d_lon,d_lat,d_x,d_y,
  PURP7_Sch,
  trip_weight_v2,
  trip_weight,
  trip_weight_fri,
  trip_weight_sat,
  trip_weight_sun,
  trip_weight_aggregated_v2
  ),

  -- Replace 'Supplemental' with 'CBS' in segment_type
  CASE 
    WHEN t.segment_type = 'Supplemental' THEN 'CBS'
    ELSE t.segment_type
  END AS segment_type_cleaned,

  -- Group origin purpose to category of 3
  CASE 
    WHEN t.o_purpose_category = 1 THEN 'Home'
    WHEN t.o_purpose_category = 2 THEN 'Work'
    WHEN t.o_purpose_category IN (995, -1) THEN 'Undefined'
    ELSE 'Other'
  END AS o_purpose_3cat,

  -- Group destination purpose to category of 3
  CASE 
    WHEN t.d_purpose_category = 1 THEN 'Home'
    WHEN t.d_purpose_category = 2 THEN 'Work'
    WHEN t.d_purpose_category IN (995, -1) THEN 'Undefined'
    ELSE 'Other'
  END AS d_purpose_3cat,

  -- Departure hour period
  CASE 
    WHEN t.depart_hour BETWEEN 6 AND 8 THEN "AM"
    WHEN t.depart_hour BETWEEN 9 AND 14 THEN "MD"
    WHEN t.depart_hour BETWEEN 15 AND 17 THEN "PM"
    ELSE "EV"
  END AS depart_per,

  -- Departure/arrival time in specific formats
  SAFE_CAST(FORMAT('%02d%02d', t.depart_hour, t.depart_minute) AS INT64) AS depart_hhm,
  t.depart_hour * 60 + t.depart_minute AS depart_mam,

  SAFE_CAST(FORMAT('%02d%02d', t.arrive_hour, t.arrive_minute) AS INT64) AS arrive_hhm,
  t.arrive_hour * 60 + t.arrive_minute AS arrive_mam,

  -- Integer distance
  CAST(FLOOR(IFNULL(t.distance_miles, 0)) AS INT64) AS INT_Dist,

  -- Optional duration column (uncomment if needed)
  -- CAST(FLOOR(IFNULL(t.duration_minutes, 0)) AS INT64) AS INT_Dur,

  -- Mode groupings
  CASE
    WHEN t.mode_type_broad = 3 AND t.num_travelers = 1 THEN 3.1
    WHEN t.mode_type_broad = 3 AND t.num_travelers = 2 THEN 3.2
    WHEN t.mode_type_broad = 3 AND t.num_travelers >= 3 THEN 3.3
    ELSE CAST(t.mode_type_broad AS FLOAT64)
  END AS mode_auto,

  -- College trips
  CASE 
      WHEN segment_type = 'College' THEN t.trip_weight_v2
      ELSE NULL
  END AS trip_weight_col_enrol,
  
  -- Non-College trips
  CASE 
      WHEN segment_type != 'College' OR segment_type IS NULL THEN t.trip_weight_v2
      ELSE NULL
  END AS trip_weight,

  -- Compute activ_dur using LAG function
  LAG(t.arrive_hour * 60 + t.arrive_minute) OVER (
      PARTITION BY t.person_id
      ORDER BY t.hh_id, t.person_id, t.trip_id
  ) AS lag_arrive_mam,
  
  CASE
      WHEN (t.depart_hour * 60 + t.depart_minute) - 
           LAG(t.arrive_hour * 60 + t.arrive_minute) OVER (
               PARTITION BY t.person_id
               ORDER BY t.hh_id, t.person_id, t.trip_id
           ) < 0
      THEN -1
      ELSE IFNULL(
          (t.depart_hour * 60 + t.depart_minute) -
          LAG(t.arrive_hour * 60 + t.arrive_minute) OVER (
              PARTITION BY t.person_id
              ORDER BY t.hh_id, t.person_id, t.trip_id
          ),
          0
      )
  END AS activ_dur,

  -- calculate purpose as text using 'PURP7'
  CASE
    WHEN PURP7 = 1 THEN 'HBW'
    WHEN PURP7 = 2 THEN 'HBSch'
    WHEN PURP7 = 3 THEN 'HBShp'
    WHEN PURP7 = 4 THEN 'HBOth'
    WHEN PURP7 = 5 THEN 'NHBW'
    WHEN PURP7 = 6 THEN 'NHBNW'
    WHEN PURP7 = 7 THEN 'HBC'
    WHEN PURP7 = 995 THEN 'Missing Response'
    ELSE 'HBOth'
  END AS PURP7_t,

  -- calculate prupose as text (more divisions) -- trip_pur_t
  CASE
    WHEN PURP7 = 4 AND trip_type = 4 THEN 'HBPb'
    WHEN PURP7 = 4 AND trip_type != 4 THEN 'HBOth'
    ELSE
      CASE
        WHEN PURP7 = 1 THEN 'HBW'
        WHEN PURP7 = 2 THEN 'HBSch'
        WHEN PURP7 = 3 THEN 'HBShp'
        WHEN PURP7 = 5 THEN 'NHBW'
        WHEN PURP7 = 6 THEN 'NHBNW'
        WHEN PURP7 = 7 THEN 'HBC'
        WHEN PURP7 = 995 THEN 'Missing Response'
        ELSE 'HBOth'
      END
  END AS trip_pur_t,

  -- Specify primary/secondary schooling
  CASE
    WHEN PURP7 = 2 AND school_type = 5 THEN 'primary'
    WHEN PURP7 = 2 AND school_type IN (6, 7) THEN 'secondary'
    WHEN PURP7 = 2 THEN 'undefined'
    ELSE NULL
  END AS HBSch_lev,

  -- Compute PURP_PER using 'depart_hour' and 'PURP7'
  CASE
      WHEN t.PURP7 = 995 OR
           CASE 
             WHEN t.depart_hour BETWEEN 6 AND 8 THEN 1
             WHEN t.depart_hour BETWEEN 9 AND 14 THEN 2
             WHEN t.depart_hour BETWEEN 15 AND 17 THEN 3
             ELSE 4
           END = 995 THEN 995
      ELSE t.PURP7 + (
        (CASE 
           WHEN t.depart_hour BETWEEN 6 AND 8 THEN 1
           WHEN t.depart_hour BETWEEN 9 AND 14 THEN 2
           WHEN t.depart_hour BETWEEN 15 AND 17 THEN 3
           ELSE 4
         END) - 1
      ) * 7
  END AS PURP_PER,

  -- calculate production CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.oCO_TAZID_USTMv3
    WHEN t.PA_AP = 'AP' THEN t.dCO_TAZID_USTMv3
    ELSE NULL
  END AS pCO_TAZID_USTMv3,
  
  -- calculate attraciton CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.dCO_TAZID_USTMv3
    WHEN t.PA_AP = 'AP' THEN t.oCO_TAZID_USTMv3
    ELSE NULL
  END AS aCO_TAZID_USTMv3,

  -- calculate production CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.oCO_TAZID_USTMv4
    WHEN t.PA_AP = 'AP' THEN t.dCO_TAZID_USTMv4
    ELSE NULL
  END AS pCO_TAZID_USTMv4,
  
  -- calculate attraciton CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.dCO_TAZID_USTMv4
    WHEN t.PA_AP = 'AP' THEN t.oCO_TAZID_USTMv4
    ELSE NULL
  END AS aCO_TAZID_USTMv4,

  -- calculate production CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.oSUBAREAID
    WHEN t.PA_AP = 'AP' THEN t.dSUBAREAID
    ELSE NULL
  END AS pSUBAREAID,
  
  -- calculate attraciton CO_TAZID
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.dSUBAREAID
    WHEN t.PA_AP = 'AP' THEN t.oSUBAREAID
    ELSE NULL
  END AS aSUBAREAID,

  -- calculate production BG
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.o_bg_2020
    WHEN t.PA_AP = 'AP' THEN t.d_bg_2020
    ELSE NULL
  END AS p_bg_2020,
  
  -- calculate attraciton BG
  CASE 
    WHEN t.PA_AP = 'PA' THEN t.d_bg_2020
    WHEN t.PA_AP = 'AP' THEN t.o_bg_2020
    ELSE NULL
  END AS a_bg_2020,

FROM trip_taz_pa_sch_purp AS t