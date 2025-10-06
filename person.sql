
-------------------------------------------------------------------------------------
-- preprocessing/setting up certain columns before final selection
-------------------------------------------------------------------------------------
--calculate number of drive to work trips and drive to work distance per person
WITH trips_filtered AS (
  SELECT
    person_id,
    trip_id,
    distance_miles
  FROM `confidential-2023-utah-hts.20250728.core_trip`
  WHERE trip_type IN (1, 6)
),
trips_count AS (
  SELECT
    person_id,
    COUNT(trip_id) AS drive_work_trips,
    SUM(distance_miles) AS drive_work_distance
  FROM trips_filtered
  GROUP BY person_id
)


-------------------------------------------------------------------------------------
-- calculate remaining trip fields using preprocessed tables from above
-------------------------------------------------------------------------------------
SELECT 
  p.* EXCEPT(Unnamed__0, segment_type,
  second_home_bg_2010,second_home_bg_2020,second_home_taz,second_home_lon,second_home_lat,second_home_x,second_home_y,
         work_bg_2010,       work_bg_2020,       work_taz,       work_lon,       work_lat,       work_x,       work_y,
       school_bg_2010,     school_bg_2020,     school_taz,     school_lon,     school_lat,     school_x,     school_y,
       person_weight,
       person_weight_fri,
       person_weight_sat,
       person_weight_sun,
       person_weight_v2,
       person_weight_aggregated_v2
  ),

  -- Replace 'Supplemental' with 'CBS' in segment_type
  CASE 
    WHEN p.segment_type = 'Supplemental' THEN 'CBS'
    ELSE p.segment_type
  END AS segment_type_cleaned,

 -- Lifegroup categorization
  CASE
    WHEN segment_type != 'College' AND age <= 3 THEN 'child'
    WHEN segment_type != 'College' AND age <= 8 THEN 'adult'
    WHEN segment_type != 'College' AND age <= 11 THEN 'senior'
    ELSE NULL
  END AS lifegroup_1TG,

 -- Group age into bins
  CASE
    WHEN segment_type != 'College' AND age BETWEEN 0 AND 3 THEN 1
    WHEN segment_type != 'College' AND age BETWEEN 4 AND 8 THEN 2
    WHEN segment_type != 'College' AND age >= 9 THEN 3
    ELSE NULL
  END AS age_3cat_1TG,


 -- _1TG Fields ARE ONLY FOR TDM use (1-TripGen)
 -- Calculate jobs by age type
  CASE
    WHEN segment_type != 'College' AND age BETWEEN 4 AND 8 THEN num_jobs
    ELSE 0
  END AS adultJobs_1TG,
  
  CASE
    WHEN segment_type != 'College' AND age >= 9 THEN num_jobs
    ELSE 0
  END AS seniorJobs_1TG,
  
  CASE
    WHEN segment_type != 'College' AND age <= 3 THEN num_jobs
    ELSE 0
  END AS childJobs_1TG,

 -- Calculate number of person trips
  CASE
    WHEN segment_type = 'College' AND num_trips > 0 THEN 1
    ELSE 0
  END AS person_made_trips_college,

 -- Calculate number of person trips
  CASE
    WHEN segment_type != 'College' AND num_trips > 0 THEN 1
    ELSE 0
  END AS person_made_trips_1TG,

  -- Calculate drive to work trip and distance
  COALESCE(t.drive_work_trips, 0) AS drive_work_trips,
  COALESCE(t.drive_work_distance, 0) AS drive_work_distance,

  -- Non-College (1-TripGen) only
  CASE 
    WHEN segment_type = 'College' THEN person_weight_v2 
    ELSE NULL
  END AS person_weight_college,

  -- Non-College (1-TripGen) only
  CASE 
    WHEN segment_type = 'College' THEN NULL 
    ELSE person_weight_v2 
  END AS hh_person_weight_1TG,
 
FROM `confidential-2023-utah-hts.20250728.core_person` AS p

LEFT JOIN trips_count AS t
ON p.person_id = t.person_id
