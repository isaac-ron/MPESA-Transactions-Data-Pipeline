-- The warehouse is split into layers, one schema each (often called the "medallion" pattern):
--
--   raw        exact copy of the source files, everything stored as TEXT, never edited (bronze)
--   staging    cleaned, typed, de-duplicated data (silver)        <- Stage 2
--   warehouse  star schema for analytics: facts + dimensions (gold) <- Stage 3
--
-- Why keep raw untouched? If a cleaning rule turns out to be wrong, you fix
-- the rule and rebuild staging from raw. You never need to ask the source
-- system for the data again.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
