-- ============================================================
-- Ride-Hailing Fleet Operations (Lambda Architecture)
-- PostgreSQL Schema Initialization
-- ============================================================

-- 1. Speed Layer: Real-time windowed zone metrics
CREATE TABLE IF NOT EXISTS realtime_zone_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    zone VARCHAR(64) NOT NULL,
    active_vehicles INTEGER NOT NULL DEFAULT 0,
    total_events INTEGER NOT NULL DEFAULT 0,
    idle_events INTEGER NOT NULL DEFAULT 0,
    idle_ratio DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    trip_count INTEGER NOT NULL DEFAULT 0,
    total_earnings NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rzm_zone_window
    ON realtime_zone_metrics (zone, window_start DESC);

-- 2. Speed Layer: Vehicle idle & operational alerts
CREATE TABLE IF NOT EXISTS fleet_alerts (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL,
    alert_type VARCHAR(64) NOT NULL,
    idle_duration_sec DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    zone VARCHAR(64),
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fleet_alerts_created
    ON fleet_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fleet_alerts_vehicle
    ON fleet_alerts (vehicle_id);

-- 3. Batch Layer: Daily vehicle profitability summary
CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    sim_day INTEGER NOT NULL,
    vehicle_id VARCHAR(64) NOT NULL,
    total_trips INTEGER NOT NULL DEFAULT 0,
    total_fare NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    fuel_cost NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    maintenance_cost NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    distance_covered DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    service_flag BOOLEAN NOT NULL DEFAULT FALSE,
    net_profit NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    is_unprofitable BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sim_day, vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_dvp_unprofitable
    ON daily_vehicle_profitability (sim_day, is_unprofitable);

-- 4. Observability: Pipeline health & failure alerts
CREATE TABLE IF NOT EXISTS pipeline_health_alerts (
    id SERIAL PRIMARY KEY,
    stage VARCHAR(64) NOT NULL,
    alert_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL DEFAULT 'WARNING',
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pha_created
    ON pipeline_health_alerts (created_at DESC);