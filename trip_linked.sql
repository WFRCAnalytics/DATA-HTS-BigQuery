WITH
-- origin TAZ (USTM v3)
origin_taz_v3 AS (
  SELECT
    t.linked_trip_id,
    SAFE_CAST(ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS INT64) AS oCO_TAZID_USTMv3
  FROM `wfrc-modeling-data.ext_rsg_hts_2023.trip_linked` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v3_taz_2021_09_22_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- destination TAZ (USTM v3)
destination_taz_v3 AS (
  SELECT
    t.linked_trip_id,
    SAFE_CAST(ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS INT64) AS dCO_TAZID_USTMv3,
  FROM `wfrc-modeling-data.ext_rsg_hts_2023.trip_linked` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v3_taz_2021_09_22_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- origin TAZ (USTM v4)
origin_taz_v4 AS (
  SELECT
    t.linked_trip_id,
    SAFE_CAST(ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS INT64) AS oCO_TAZID_USTMv4,
    SAFE_CAST(ARRAY_AGG(taz.SUBAREAID LIMIT 1)[OFFSET(0)] AS INT64) AS oSUBAREAID
  FROM `wfrc-modeling-data.ext_rsg_hts_2023.trip_linked` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v4_taz_2025_07_29_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- destination TAZ (USTM v4)
destination_taz_v4 AS (
  SELECT

    t.linked_trip_id,
    SAFE_CAST(ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS INT64) AS dCO_TAZID_USTMv4,
    SAFE_CAST(ARRAY_AGG(taz.SUBAREAID LIMIT 1)[OFFSET(0)] AS INT64) AS dSUBAREAID
  FROM `wfrc-modeling-data.ext_rsg_hts_2023.trip_linked` AS t
  JOIN `wfrc-modeling-data.prd_tdm_taz.ustm_v4_taz_2025_07_29_geo` AS taz
    ON ST_INTERSECTS(st_geogpoint(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- select columns and join geographies
trips_with_taz AS (
  SELECT
    t.* EXCEPT(trip_weight_new, o_lon, o_lat, d_lon, d_lat),
    ot3.oCO_TAZID_USTMv3,
    dt3.dCO_TAZID_USTMv3,
    ot4.oCO_TAZID_USTMv4,
    dt4.dCO_TAZID_USTMv4,
    ot4.oSUBAREAID,
    dt4.dSUBAREAID,
    t.trip_weight_new AS trip_weight
  FROM `wfrc-modeling-data.ext_rsg_hts_2023.trip_linked` AS t
  LEFT JOIN origin_taz_v3      AS ot3 USING (linked_trip_id)
  LEFT JOIN destination_taz_v3 AS dt3 USING (linked_trip_id)
  LEFT JOIN origin_taz_v4      AS ot4 USING (linked_trip_id)
  LEFT JOIN destination_taz_v4 AS dt4 USING (linked_trip_id)
),

trips_with_purposes AS (
  SELECT
    *,
    -- o_purpose_category3
    CASE
      WHEN o_purpose_category = 1 THEN 'Home'
      WHEN o_purpose_category IN (2,3) THEN 'Work'
      WHEN o_purpose_category IN (995,-1) THEN 'Undefined'
      ELSE 'Other'
    END AS o_purpose_category3,

    -- d_purpose_category3
    CASE
      WHEN d_purpose_category = 1 THEN 'Home'
      WHEN d_purpose_category IN (2,3) THEN 'Work'
      WHEN d_purpose_category IN (995,-1) THEN 'Undefined'
      ELSE 'Other'
    END AS d_purpose_category3,

    -- o_purpose_type
    CASE o_purpose_category
      WHEN 1 THEN 'home'
      WHEN 2 THEN 'work'
      WHEN 3 THEN 'work-related'
      WHEN 4 THEN 'school'
      WHEN 5 THEN 'school-related'
      WHEN 7 THEN 'shop'
      WHEN 6 THEN 'escort'
      WHEN 8 THEN 'meal'
      WHEN 9 THEN 'social-rec'
      WHEN 10 THEN 'errand'
      WHEN 11 THEN 'change-mode'
      WHEN 12 THEN 'overnight'
      WHEN 13 THEN 'other'
      ELSE NULL
    END AS o_purpose_type,

    -- d_purpose_type
    CASE d_purpose_category
      WHEN 1 THEN 'home'
      WHEN 2 THEN 'work'
      WHEN 3 THEN 'work-related'
      WHEN 4 THEN 'school'
      WHEN 5 THEN 'school-related'
      WHEN 7 THEN 'shop'
      WHEN 6 THEN 'escort'
      WHEN 8 THEN 'meal'
      WHEN 9 THEN 'social-rec'
      WHEN 10 THEN 'errand'
      WHEN 11 THEN 'change-mode'
      WHEN 12 THEN 'overnight'
      WHEN 13 THEN 'other'
      ELSE NULL
    END AS d_purpose_type
  FROM trips_with_taz
),

trips_with_mode AS (
  SELECT
    *,
    -- linked_trip_mode_t
    CASE linked_trip_mode
      WHEN -1 THEN 'Missing Response'
      WHEN 1  THEN 'School Bus'
      WHEN 2  THEN 'Drive-Transit'
      WHEN 5  THEN 'Walk-Transit'
      WHEN 8  THEN 'Shared-Ride 3+'
      WHEN 9  THEN 'Shared-Ride 2'
      WHEN 10 THEN 'Drive-Alone'
      WHEN 11 THEN 'Bike'
      WHEN 12 THEN 'Scooter'
      WHEN 13 THEN 'Taxi'
      WHEN 14 THEN 'TNC'
      WHEN 15 THEN 'Walk'
      WHEN 16 THEN 'Long Distance'
      WHEN 17 THEN 'Other'
      ELSE NULL
    END AS linked_trip_mode_t,
  FROM trips_with_purposes
),

trips_with_pa_ap AS (
  SELECT
    *,
    -- PA_AP calculation
    CASE
      WHEN o_purpose_category3 = 'Home' THEN 'PA'
      WHEN d_purpose_category3 = 'Home' THEN 'AP'
      WHEN o_purpose_category3 = 'Work' AND d_purpose_category3 = 'Other' THEN 'PA'
      WHEN o_purpose_category3 = 'Other' AND d_purpose_category3 = 'Work' THEN 'AP'
      WHEN o_purpose_category3 = 'Undefined' OR d_purpose_category3 = 'Undefined' THEN 'Undefined'
      ELSE 'PA'
    END AS PA_AP
  FROM trips_with_mode
),

trips_with_times AS (
  SELECT
    *,
    -- Depart period
    CASE
      WHEN depart_hour BETWEEN 6 AND 8 THEN 'AM'
      WHEN depart_hour BETWEEN 9 AND 14 THEN 'MD'
      WHEN depart_hour BETWEEN 15 AND 17 THEN 'PM'
      ELSE 'EV'
    END AS depart_per,

    -- Arrive period
    CASE
      WHEN arrive_hour BETWEEN 6 AND 8 THEN 'AM'
      WHEN arrive_hour BETWEEN 9 AND 14 THEN 'MD'
      WHEN arrive_hour BETWEEN 15 AND 17 THEN 'PM'
      ELSE 'EV'
    END AS arrive_per,

    -- Depart/arrive HHMM
    SAFE_CAST(FORMAT('%02d%02d', depart_hour, depart_minute) AS INT64) AS depart_hhm,
    depart_hour * 60 + depart_minute AS depart_mam,
    SAFE_CAST(FORMAT('%02d%02d', arrive_hour, arrive_minute) AS INT64) AS arrive_hhm,
    arrive_hour * 60 + arrive_minute AS arrive_mam
  FROM trips_with_pa_ap
),

trips_with_school AS (
  SELECT
    *,
    -- PURP7_t calculation (rename + special case for HBO)
    CASE
      WHEN Model_Purpose = 'HBO' THEN 'HBOth'
      ELSE Model_Purpose
    END AS PURP7_t,

    -- rename depart_seconds
    depart_seconds AS depart_second,
   
    --- school level
    CASE
      WHEN Model_Purpose = 'HBSch' AND school_type = 5 THEN 'primary'
      WHEN Model_Purpose = 'HBSch' AND school_type IN (6, 7) THEN 'secondary'
      WHEN Model_Purpose = 'HBSch' AND school_type NOT IN (5, 6, 7) THEN 'undefined'
      ELSE 'NULL'
    END AS HBSch_lev
  FROM trips_with_times
),

-- join unlinked trips columns for non-linked trips
trips_with_unlinked AS (
  SELECT
    t.*,
    u.trip_id,
    u.speed_mph,
    u.speed_mph_collected,
    u.speed_flag,
    u.distance_meters,
    u.distance_meters_collected,
    u.distance_miles,
    u.distance_miles_collected,
    u.park_location,
    u.park_type,
    u.park_pay,
    u.park_cost,
    u.ev_charge_station,
    u.ev_charge_station_level_1,
    u.ev_charge_station_level_2,
    u.ev_charge_station_level_998,
    u.ev_charge_station_decision,
    u.tnc_type,
    u.taxi_type,
    u.taxi_pay,
    u.transit_type,
    u.num_travelers,
    u.num_hh_travelers,
    u.num_non_hh_travelers,
    u.driver
  FROM trips_with_school AS t
  LEFT JOIN `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_trip` AS u
    ON CAST(t.linked_trip_id / 1000 AS INT64) = u.trip_id
    AND t.joint_status = 1
),

-- production/attraction zones
trips_with_pa_zones AS (
  SELECT
    *,
    -- production / attraction for USTMv3
    CASE
      WHEN PA_AP = 'PA' THEN oCO_TAZID_USTMv3
      WHEN PA_AP = 'AP' THEN dCO_TAZID_USTMv3
      ELSE NULL
    END AS pCO_TAZID_USTMv3,

    CASE
      WHEN PA_AP = 'PA' THEN dCO_TAZID_USTMv3
      WHEN PA_AP = 'AP' THEN oCO_TAZID_USTMv3
      ELSE NULL
    END AS aCO_TAZID_USTMv3,

    -- production / attraction for USTMv4
    CASE
      WHEN PA_AP = 'PA' THEN oCO_TAZID_USTMv4
      WHEN PA_AP = 'AP' THEN dCO_TAZID_USTMv4
      ELSE NULL
    END AS pCO_TAZID_USTMv4,

    CASE
      WHEN PA_AP = 'PA' THEN dCO_TAZID_USTMv4
      WHEN PA_AP = 'AP' THEN oCO_TAZID_USTMv4
      ELSE NULL
    END AS aCO_TAZID_USTMv4,

    -- production / attraction for USTMv4
    CASE
      WHEN PA_AP = 'PA' THEN oSUBAREAID
      WHEN PA_AP = 'AP' THEN dSUBAREAID
      ELSE NULL
    END AS pSUBAREAID,

    CASE
      WHEN PA_AP = 'PA' THEN dSUBAREAID
      WHEN PA_AP = 'AP' THEN oSUBAREAID
      ELSE NULL
    END AS aSUBAREAID

  FROM trips_with_unlinked
)


SELECT
  linked_trip_id, trip_id, hh_id, person_id, day_id, day_weight,
  person_num, day_num,
  participation_group, diary_platform,
  o_purpose, o_purpose_category, o_purpose_type, o_purpose_category3, o_purpose_type_rsg,
  depart_time, depart_date, depart_hour, depart_minute, depart_second, depart_per, depart_hhm, depart_mam,
  d_purpose, d_purpose_category, d_purpose_type, d_purpose_category3, d_purpose_type_rsg,
  arrive_time, arrive_date, arrive_hour, arrive_minute, arrive_second, arrive_per, arrive_hhm, arrive_mam,
  home_distance, duration_minutes, dwell_mins,
  hh_member_1, hh_member_2, hh_member_3, hh_member_4, hh_member_5, hh_member_6, hh_member_7, hh_member_8, hh_member_9, hh_member_10, hh_member_11, hh_member_12, hh_member_13,
  joint_status, joint_trip_id, joint_trip_num, joint_num_participants,
  escort_category, outbound,
  primdest_penalty, trip_adjustment_factor,
  speed_mph, speed_mph_collected, speed_flag,
  distance_meters, distance_meters_collected, distance_miles, distance_miles_collected,
  park_location, park_type, park_pay, park_cost,
  ev_charge_station, ev_charge_station_level_1, ev_charge_station_level_2, ev_charge_station_level_998, ev_charge_station_decision,
  tnc_type, taxi_type, taxi_pay,
  transit_type,
  num_travelers, num_hh_travelers, num_non_hh_travelers, 
  driver,
  linked_trip_mode, linked_trip_mode_t,
  linked_trip_weight, linked_trip_num, 
  tour_num, tour_id, 
  trip_purp_RSG, TMR_Purpose, PURP7_t, 
  person_type, person_cat, 
  school_type, HBSch_lev,
  PA_AP,
  oCO_TAZID_USTMv3, dCO_TAZID_USTMv3, pCO_TAZID_USTMv3, aCO_TAZID_USTMv3,
  oCO_TAZID_USTMv4, dCO_TAZID_USTMv4, pCO_TAZID_USTMv4, aCO_TAZID_USTMv4,
  pSUBAREAID, aSUBAREAID,
  trip_weight
FROM trips_with_pa_zones;














