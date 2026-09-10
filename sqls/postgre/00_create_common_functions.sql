-- Doris metadata public functions
-- create an automatic update mtimestamp trigger function reused for all tables

DROP FUNCTION IF EXISTS update_mtimestamp() CASCADE;

CREATE OR REPLACE FUNCTION update_mtimestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.mtimestamp = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_mtimestamp() IS 'Updates mtimestamp to the current timestamp automatically.';
