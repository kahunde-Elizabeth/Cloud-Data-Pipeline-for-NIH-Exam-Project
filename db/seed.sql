-- Synthetic reference data (Annex B). Safe to re-run.
USE nidh;

-- 3,200 facilities F0001-F3200 spread over districts D001-D150
INSERT INTO facilities (facility_id, district_id, name)
SELECT 'F' || lpad(i::STRING, 4, '0'),
       'D' || lpad((((i - 1) % 150) + 1)::STRING, 3, '0'),
       'Synthetic facility ' || i::STRING
FROM generate_series(1, 3200) AS g(i)
ON CONFLICT (facility_id) DO NOTHING;

-- the two roles from the design (used by the read API in a later step)
INSERT INTO users (username, role, district_id) VALUES
    ('officer_d001', 'district_officer', 'D001'),
    ('analyst_national', 'national_analyst', NULL)
ON CONFLICT (username) DO NOTHING;
