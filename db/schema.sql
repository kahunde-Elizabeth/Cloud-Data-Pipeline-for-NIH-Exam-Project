-- NIDH operational schema for CockroachDB (DSC3219 project)
-- Matches the M1-d data model. Loaded once by the db-init container (safe to re-run).
-- Synthetic data only (Annex B): child IDs start with SYN-, facilities F0001..., districts D001-D150.

CREATE DATABASE IF NOT EXISTS nidh;
USE nidh;

-- ---------------------------------------------------------------- reference data
CREATE TABLE IF NOT EXISTS facilities (
    facility_id  STRING PRIMARY KEY,                 -- F0001 ...
    district_id  STRING NOT NULL,                    -- D001 - D150
    name         STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS children (
    child_id         STRING PRIMARY KEY CHECK (child_id LIKE 'SYN-%'),
    birth_date       DATE,                           -- NULL when the tablet did not send it
    sex              STRING,
    home_facility    STRING REFERENCES facilities (facility_id),
    caregiver_phone  BYTES                           -- encrypted by the application; mock only
);

CREATE TABLE IF NOT EXISTS users (
    user_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username     STRING NOT NULL UNIQUE,
    role         STRING NOT NULL CHECK (role IN ('district_officer', 'national_analyst')),
    district_id  STRING                              -- scope for district officers, NULL for analysts
);

-- ---------------------------------------------------------------- write path
CREATE TABLE IF NOT EXISTS vaccination_records (
    record_id        UUID PRIMARY KEY,               -- client-generated idempotency key (UUID v4; v5 for SMS)
    child_id         STRING NOT NULL REFERENCES children (child_id),
    facility_id      STRING NOT NULL REFERENCES facilities (facility_id),
    district_id      STRING NOT NULL,
    vaccine_code     STRING NOT NULL,
    dose_no          INT2   NOT NULL CHECK (dose_no BETWEEN 1 AND 5),
    batch_no         STRING,
    device_ts        TIMESTAMPTZ NOT NULL,           -- tablet clock, kept as recorded
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),  -- server clock, authoritative for ordering
    clock_skew_flag  BOOL NOT NULL DEFAULT false,
    skew_seconds     INT,
    source           STRING NOT NULL CHECK (source IN ('tablet', 'sms')),
    version          INT NOT NULL DEFAULT 1,         -- optimistic concurrency
    updated_hlc      DECIMAL,                        -- last-writer-wins ordering for edits
    updated_by       STRING,                         -- facility_id, deterministic tie-break
    CONSTRAINT uq_child_dose UNIQUE (child_id, vaccine_code, dose_no),
    INDEX idx_district_time (district_id, device_ts),
    INDEX idx_received (received_at) USING HASH      -- avoids a hot range on time-ordered inserts
);

CREATE TABLE IF NOT EXISTS record_history (
    history_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id    UUID NOT NULL REFERENCES vaccination_records (record_id),
    version      INT NOT NULL,
    old_values   JSONB,
    new_values   JSONB,
    edit_hlc     DECIMAL,
    facility_id  STRING,
    outcome      STRING NOT NULL CHECK (outcome IN ('applied', 'lost_conflict', 'flagged')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX idx_hist_record (record_id)
);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id    UUID,
    reason       STRING NOT NULL CHECK (reason IN ('invalid_format', 'duplicate_dose', 'poison')),
    detail       STRING,                              -- the exact rule that failed
    raw_payload  JSONB,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- read path
CREATE TABLE IF NOT EXISTS district_coverage (
    district_id   STRING NOT NULL,
    vaccine_code  STRING NOT NULL,
    dose_no       INT2   NOT NULL,
    iso_week      STRING NOT NULL,                   -- e.g. 2026-W47
    doses         INT    NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (district_id, vaccine_code, dose_no, iso_week)
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id   UUID NOT NULL REFERENCES users (user_id),
    action    STRING NOT NULL,
    resource  STRING NOT NULL,
    params    JSONB,
    at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX idx_audit_at (at) USING HASH
);

-- ---------------------------------------------------------------- sensors
CREATE TABLE IF NOT EXISTS sensor_readings_1min (
    sensor_id     STRING NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    facility_id   STRING NOT NULL REFERENCES facilities (facility_id),
    min_c         DECIMAL(4,1),
    avg_c         DECIMAL(4,1),
    max_c         DECIMAL(4,1),
    n_readings    INT2,
    out_of_range  BOOL NOT NULL DEFAULT false,       -- outside 2-8 C
    PRIMARY KEY (sensor_id, window_start)
) WITH (ttl_expire_after = '90 days');               -- row-level TTL keeps sensor storage bounded

CREATE TABLE IF NOT EXISTS sensor_status (
    sensor_id     STRING PRIMARY KEY,
    facility_id   STRING NOT NULL REFERENCES facilities (facility_id),
    last_seen_at  TIMESTAMPTZ,                       -- server time of the last reading
    status        STRING NOT NULL DEFAULT 'online' CHECK (status IN ('online', 'offline'))
);
-- added in M3 step 4 (ALTER so that an existing cluster is upgraded in place)
ALTER TABLE sensor_status ADD COLUMN IF NOT EXISTS temp_state STRING NOT NULL DEFAULT 'ok'
    CHECK (temp_state IN ('ok', 'too_warm', 'too_cold'));
ALTER TABLE sensor_status ADD COLUMN IF NOT EXISTS last_temp_c DECIMAL(4,1);

-- Alert log (data-flow diagram: "Mock SMS + alert log"). Also the outbox for the alerts topic:
-- an alert is saved in the same transaction as the reading that caused it, and published
-- afterwards, so no alert can be lost between the database and the stream.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id     UUID PRIMARY KEY,                   -- uuid5(sensor, kind, moment): replay-safe
    sensor_id    STRING NOT NULL,
    facility_id  STRING NOT NULL REFERENCES facilities (facility_id),
    kind         STRING NOT NULL CHECK (kind IN ('too_warm', 'too_cold', 'back_in_range',
                                                 'offline', 'back_online')),
    detail       STRING,
    temp_c       DECIMAL(4,1),
    at           TIMESTAMPTZ NOT NULL,               -- when the condition started
    published    BOOL NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX idx_alerts_facility (facility_id, at)
);
CREATE INDEX IF NOT EXISTS idx_alerts_unpublished ON alerts (created_at) WHERE NOT published;

-- ---------------------------------------------------------------- coordination
CREATE TABLE IF NOT EXISTS aggregator_lease (
    lease_name  STRING PRIMARY KEY,
    holder      STRING NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL
);

-- ---------------------------------------------------------------- the two key statements
-- 1) Batch insert (one transaction per batch; duplicates silently skipped):
--    INSERT INTO vaccination_records (...) VALUES (...), (...), ...
--    ON CONFLICT DO NOTHING;
--    (A logical duplicate - same child/vaccine/dose, new record_id - is also skipped here;
--     the processor detects it by comparing inserted rows and writes it to rejects as 'duplicate_dose'.)
--
-- 2) Edit with optimistic concurrency (serializable transaction, retry on SQLSTATE 40001):
--    UPDATE vaccination_records
--       SET batch_no = $new, version = version + 1, updated_hlc = $hlc, updated_by = $facility
--     WHERE record_id = $id AND version = $expected_version;
--    -- 0 rows updated => conflict: apply last-writer-wins on (updated_hlc, updated_by),
--    -- and write the losing edit to record_history with outcome = 'lost_conflict'.
