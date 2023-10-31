SET @log_date = '{{ params.log_date }}';
call sp_rpt_data_quality(0 ,1, null,@log_date);