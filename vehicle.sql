SELECT 
  v.* EXCEPT(segment_type, hh_weight, hh_weight_fri, hh_weight_sat, hh_weight_sun),

  -- Replace 'Supplemental' with 'CBS' in segment_type
  CASE 
    WHEN v.segment_type = 'Supplemental' THEN 'CBS'
    ELSE v.segment_type
  END AS segment_type_cleaned
  
FROM `confidential-2023-utah-hts.20250728.core_vehicle` AS v
