CREATE TABLE IF NOT EXISTS kpi_lte_calculation_60 (
        date_id VARCHAR,
		ropendtime VARCHAR,
		moid VARCHAR,
		nodeVARCHAR,  
        utc_timestamp_day TIMESTAMP WITHOUT TIME ZONE, 
        local_timestamp_day TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
        pm_stats_reliability DOUBLE PRECISION, 
        PRIMARY KEY (date_id, ropendtime, moid, node, local_timestamp_day));