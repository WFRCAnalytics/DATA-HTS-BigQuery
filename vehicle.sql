SELECT 
  v.* EXCEPT(segment_type, hh_weight, hh_weight_fri, hh_weight_sat, hh_weight_sun),

  -- Replace 'Supplemental' with 'CBS' in segment_type
  CASE 
    WHEN v.segment_type = 'Supplemental' THEN 'CBS'
    ELSE v.segment_type
  END AS segment_type_cleaned
  
FROM `wfrc-modeling-data.src_rsg_household_travel_survey_2023.core_vehicle` AS v
