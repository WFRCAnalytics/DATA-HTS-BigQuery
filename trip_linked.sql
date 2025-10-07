WITH
-- origin TAZ (USTM v3)
origin_taz_v3 AS (
  SELECT
    t.linked_trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS oCO_TAZID_USTMv3
  FROM `confidential-2023-utah-hts.20250728.trips_adjusted` AS t
  JOIN `confidential-2023-utah-hts.geometries.ustm_v3_taz_2021_09_22_geog` AS taz
    ON ST_INTERSECTS(ST_GEOGPOINT(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- destination TAZ (USTM v3)
destination_taz_v3 AS (
  SELECT
    t.linked_trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS dCO_TAZID_USTMv3
  FROM `confidential-2023-utah-hts.20250728.trips_adjusted` AS t
  JOIN `confidential-2023-utah-hts.geometries.ustm_v3_taz_2021_09_22_geog` AS taz
    ON ST_INTERSECTS(ST_GEOGPOINT(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- origin TAZ (USTM v4)
origin_taz_v4 AS (
  SELECT
    t.linked_trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS oCO_TAZID_USTMv4
  FROM `confidential-2023-utah-hts.20250728.trips_adjusted` AS t
  JOIN `confidential-2023-utah-hts.geometries.ustm_v4_taz_2025_07_29_geog` AS taz
    ON ST_INTERSECTS(ST_GEOGPOINT(t.o_lon, t.o_lat), taz.geometry)
  GROUP BY t.linked_trip_id
),

-- destination TAZ (USTM v4)
destination_taz_v4 AS (
  SELECT
    t.linked_trip_id,
    ARRAY_AGG(taz.CO_TAZID LIMIT 1)[OFFSET(0)] AS dCO_TAZID_USTMv4
  FROM `confidential-2023-utah-hts.20250728.trips_adjusted` AS t
  JOIN `confidential-2023-utah-hts.geometries.ustm_v4_taz_2025_07_29_geog` AS taz
    ON ST_INTERSECTS(ST_GEOGPOINT(t.d_lon, t.d_lat), taz.geometry)
  GROUP BY t.linked_trip_id
)

SELECT
  t.* EXCEPT(trip_weight_new),
  t.trip_weight_new AS trip_weight_1TG,
  ot3.oCO_TAZID_USTMv3,
  dt3.dCO_TAZID_USTMv3,
  ot4.oCO_TAZID_USTMv4,
  dt4.dCO_TAZID_USTMv4
FROM `confidential-2023-utah-hts.20250728.trips_adjusted` AS t
LEFT JOIN origin_taz_v3      AS ot3 USING (linked_trip_id)
LEFT JOIN destination_taz_v3 AS dt3 USING (linked_trip_id)
LEFT JOIN origin_taz_v4      AS ot4 USING (linked_trip_id)
LEFT JOIN destination_taz_v4 AS dt4 USING (linked_trip_id);
