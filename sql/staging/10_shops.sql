-- Shop master: raw.shops -> staging.shops.
-- The source sends a clean snapshot, so this is mostly trimming and typing.
-- A till listed twice would violate the PRIMARY KEY and fail the build loudly.

TRUNCATE staging.shops;

INSERT INTO staging.shops
    (till_number, shop_name, business_category, town, county, opened_on, _source_file, _source_row)
SELECT btrim(till_number),
       btrim(shop_name),
       btrim(business_category),
       btrim(town),
       btrim(county),
       staging.try_timestamp(btrim(opened_on))::DATE,
       _source_file,
       _source_row
FROM raw.shops;
