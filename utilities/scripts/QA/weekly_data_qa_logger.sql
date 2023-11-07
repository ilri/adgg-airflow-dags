SET @log_year = {{ params.log_year }};
SET @log_week = {{ params.log_week }};

call sp_rpt_data_quality_weekly(@log_year, @log_week)