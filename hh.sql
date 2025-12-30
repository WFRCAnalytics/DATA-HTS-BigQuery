
-------------------------------------------------------------------------------------
-- preprocessing/setting up certain columns before final selection
-------------------------------------------------------------------------------------
-- prep person table with child, adult, or senior designations
  -- 0<child<18
  -- 18<=adult<65
  -- 65<=senior
WITH person_lifegroup AS (
  SELECT
    hh_id,
    person_id,
    age,
    CASE
      WHEN age <= 3 THEN 'child'
      WHEN age <= 8 THEN 'adult'
      WHEN age <= 11 THEN 'senior'
      ELSE NULL
    END AS lifegroup
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_person`
),

-- calculate lifecycles
  -- lifecycle 1 has no children and no seniors, only adults 
  -- lifecycle 2 has no seniors, only adults with our without children
  -- lifecycle 3 has seniors with our without children, no adults
household_lifecycle AS (
  SELECT
    hh_id,
    MAX(CASE WHEN lifegroup = 'child' THEN 1 ELSE 0 END) AS has_child,
    MAX(CASE WHEN lifegroup = 'adult' THEN 1 ELSE 0 END) AS has_adult,
    MAX(CASE WHEN lifegroup = 'senior' THEN 1 ELSE 0 END) AS has_senior,

    CASE
      -- LC1: only adults
      WHEN 
        MAX(CASE WHEN lifegroup = 'adult' THEN 1 ELSE 0 END) = 1 AND
        MAX(CASE WHEN lifegroup = 'child' THEN 1 ELSE 0 END) = 0 AND
        MAX(CASE WHEN lifegroup = 'senior' THEN 1 ELSE 0 END) = 0
      THEN 1

      -- LC2: adults and children only
      WHEN 
        MAX(CASE WHEN lifegroup = 'adult' THEN 1 ELSE 0 END) = 1 AND
        MAX(CASE WHEN lifegroup = 'child' THEN 1 ELSE 0 END) = 1 AND
        MAX(CASE WHEN lifegroup = 'senior' THEN 1 ELSE 0 END) = 0
      THEN 2

      -- LC3: subset of only child and/or senior (no adults)
      WHEN 
        MAX(CASE WHEN lifegroup = 'adult' THEN 1 ELSE 0 END) = 0 AND
        (MAX(CASE WHEN lifegroup = 'child' THEN 1 ELSE 0 END) = 1 OR
         MAX(CASE WHEN lifegroup = 'senior' THEN 1 ELSE 0 END) = 1)
      THEN 3

      -- fallback: LC3
      ELSE 3
    END AS lifecycle
  FROM person_lifegroup
  GROUP BY hh_id
),

-- summarize lifecycles
household_lifegroup_counts AS (
  SELECT
    hh_id,
    SUM(CASE WHEN age <= 3 THEN 1 ELSE 0 END) AS hh_children,
    SUM(CASE WHEN age > 3 AND age <= 8 THEN 1 ELSE 0 END) AS hh_adults,
    SUM(CASE WHEN age > 8 AND age <= 11 THEN 1 ELSE 0 END) AS hh_seniors
  FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_person`
  WHERE age IS NOT NULL
  GROUP BY hh_id
)


-------------------------------------------------------------------------------------
-- calculate remaining trip fields using preprocessed tables from above
-------------------------------------------------------------------------------------
SELECT 
  a.* EXCEPT (Unnamed__0,home_taz,home_lat,home_lon,home_x,home_y,sample_home_lat,sample_home_lon,segment_type,seasonal_res,seasonal_res_in_region,residence_months_0,hh_weight,hh_weight_fri, hh_weight_sat,hh_weight_sun,hh_weight_v2,hh_weight_aggregated_v2),

  -- completed_follow_on
  CASE 
    WHEN b.hh_id IS NOT NULL THEN 1 
    ELSE 0 
  END AS completed_followon_hh,

  -- has_* fields
  GREATEST(a.num_complete_tue, a.num_complete_wed, a.num_complete_thu) AS has_TuTh,
  GREATEST(a.num_complete_mon, a.num_complete_tue, a.num_complete_wed, a.num_complete_thu, a.num_complete_fri) AS has_MoFr,
  GREATEST(a.num_complete_sat, a.num_complete_sun) AS has_SaSu,

  -- check fields
  CASE 
    WHEN ROUND(a.home_lon, 1) = ROUND(a.seasonal_res_lon, 1) THEN 1 
    ELSE 0 
  END AS tmp_seasonal_res_lon,

  CASE 
    WHEN ROUND(a.home_lat, 1) = ROUND(a.seasonal_res_lat, 1) THEN 1 
    ELSE 0 
  END AS tmp_seasonal_res_lat,

  CASE 
    WHEN ROUND(a.home_lon, 1) = ROUND(a.seasonal_res_lon, 1)
         AND ROUND(a.home_lat, 1) = ROUND(a.seasonal_res_lat, 1) THEN 1 
    ELSE 0 
  END AS chk_seasonal_res,

  -- segment_type cleaned
  CASE 
    WHEN a.segment_type = 'Supplemental' THEN 'CBS' 
    ELSE a.segment_type 
  END AS segment_type_cleaned,

  -- seasonal_res cleaned
  CASE
    WHEN a.seasonal_res_in_region = 0 AND a.seasonal_res IN (0, 1) THEN 2
    WHEN a.seasonal_res_in_region = 1 AND a.seasonal_res = 0 AND ROUND(a.home_lat, 1) = ROUND(a.seasonal_res_lat, 1) AND ROUND(a.home_lon, 1) = ROUND(a.seasonal_res_lon, 1) THEN 1
    WHEN a.seasonal_res_in_region = 1 AND a.seasonal_res = 0 THEN 2
    WHEN a.seasonal_res_in_region = 1 AND a.seasonal_res = 1 AND (ROUND(a.home_lat, 1) != ROUND(a.seasonal_res_lat, 1) OR ROUND(a.home_lon, 1) != ROUND(a.seasonal_res_lon, 1)) THEN 2
    WHEN a.seasonal_res_in_region = 1 AND a.seasonal_res = 2 AND ROUND(a.home_lat, 1) = ROUND(a.seasonal_res_lat, 1) AND ROUND(a.home_lon, 1) = ROUND(a.seasonal_res_lon, 1) THEN 1
    ELSE a.seasonal_res
  END AS seasonal_res_cleaned,

  -- seasonal_res_in_region cleaned
  CASE 
    WHEN a.seasonal_res = 2 AND a.seasonal_res_in_region = 995 THEN 0 
    ELSE a.seasonal_res_in_region 
  END AS seasonal_res_in_region_cleaned,
    
  -- residence_months_0 cleaned (full logic from Python)
  -- residence_months_0: Months living at current residence: All 12 months
  CASE
    WHEN ARRAY_LENGTH(ARRAY(
      SELECT x FROM UNNEST([
        a.residence_months_1, a.residence_months_2, a.residence_months_3, a.residence_months_4,
        a.residence_months_5, a.residence_months_6, a.residence_months_7, a.residence_months_8,
        a.residence_months_9, a.residence_months_10, a.residence_months_11, a.residence_months_12
      ]) AS x
      WHERE x = 995
    )) > 0 THEN 995

    WHEN a.residence_months_0 = 0 AND ARRAY_LENGTH(ARRAY(
      SELECT x FROM UNNEST([
        a.residence_months_1, a.residence_months_2, a.residence_months_3, a.residence_months_4,
        a.residence_months_5, a.residence_months_6, a.residence_months_7, a.residence_months_8,
        a.residence_months_9, a.residence_months_10, a.residence_months_11, a.residence_months_12
      ]) AS x
      WHERE x != 1
    )) = 0 THEN 1

    ELSE a.residence_months_0
  END AS residence_months_0_cleaned,

  -- Additional calculated columns
  LEAST(a.num_people, 6) AS hhsize_6cat,
  LEAST(a.num_workers, 3) AS workers_4cat,
  (
    CASE
      WHEN a.residence_months_0 = 995 THEN 995
      ELSE ARRAY_LENGTH(ARRAY(
        SELECT x FROM UNNEST([
          a.residence_months_1, a.residence_months_2, a.residence_months_3, a.residence_months_4,
          a.residence_months_5, a.residence_months_6, a.residence_months_7, a.residence_months_8,
          a.residence_months_9, a.residence_months_10, a.residence_months_11, a.residence_months_12
        ]) AS x
        WHERE x IN (0, 1)
      ))
    END
  ) AS num_months,

  LEAST(a.num_vehicles, 3) AS num_vehicles_4cat,

  CASE
    WHEN a.num_vehicles >= a.num_workers THEN 'Sufficient'
    WHEN a.num_vehicles > 0 AND a.num_vehicles < a.num_workers THEN 'Insufficient'
    WHEN a.num_vehicles = 0 THEN 'No Vehicles'
    ELSE NULL
  END AS autosuf_wk,

  -- Lifecycle
  lc.lifecycle,
  cnt.hh_children,
  cnt.hh_adults,
  cnt.hh_seniors,

  -- College trips
  CASE 
      WHEN segment_type = 'College' THEN a.hh_weight_v2
      ELSE NULL
  END AS hh_weight_col_enrol,
  
  -- Non-College trips
  CASE 
      WHEN segment_type != 'College' OR segment_type IS NULL THEN a.hh_weight_v2
      ELSE NULL
  END AS hh_weight,

  -- spatial join fields
  tazv3.CO_TAZID AS hCO_TAZID_USTMv3,
  CAST(tazv3.SUBAREAID AS INT64) AS hSUBAREAID_USTMv3,
  tazv4.CO_TAZID AS hCO_TAZID_USTMv4,
  CAST(tazv4.SUBAREAID AS INT64) AS hSUBAREAID_USTMv4

FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_hh` AS a

LEFT JOIN `wfrc-modeling-data.src_rsg_household_travel_survey_2023.followon_hh` AS b
  ON a.hh_id = b.hh_id
LEFT JOIN household_lifecycle AS lc
  ON a.hh_id = lc.hh_id
LEFT JOIN household_lifegroup_counts AS cnt
  ON a.hh_id = cnt.hh_id
LEFT JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v3_taz_2021_09_22_geo` AS tazv3
  ON ST_WITHIN(st_geogpoint(a.home_lon, a.home_lat), tazv3.geometry)
LEFT JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v4_taz_2025_07_29_geo` AS tazv4
  ON ST_WITHIN(st_geogpoint(a.home_lon, a.home_lat), tazv4.geometry)