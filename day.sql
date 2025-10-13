SELECT 
  d.* EXCEPT(Unnamed__0, segment_type,
  day_weight,
  day_weight_fri,
  day_weight_sat,
  day_weight_sun,
  day_weight_v2,
  day_weight_aggregated_v2
  ),

  -- Replace 'Supplemental' with 'CBS' in segment_type
  CASE 
    WHEN d.segment_type = 'Supplemental' THEN 'CBS'
    ELSE d.segment_type
  END AS segment_type_cleaned,

  -- College trips
  CASE 
      WHEN segment_type = 'College' THEN d.day_weight_v2
      ELSE NULL
  END AS day_weight_col_enrol,
  
  -- Non-College trips
  CASE 
      WHEN segment_type != 'College' OR segment_type IS NULL THEN d.day_weight_v2
      ELSE NULL
  END AS day_weight
  
FROM `confidential-2023-utah-hts.20250728.core_day` AS d
