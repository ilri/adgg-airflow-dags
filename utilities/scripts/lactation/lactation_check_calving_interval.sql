SET @uuid = '{{ params.uuid }}';

SELECT min_value,max_value INTO @min_calving_interval,@max_calving_interval FROM interface_limit_parameters where category = 'calving_interval' LIMIT 1;


-- calving interval
UPDATE reports.staging_calving_data a
JOIN (SELECT id FROM reports.staging_calving_data WHERE uuid = @uuid AND parity>1 AND calving_interval NOT BETWEEN @min_calving_interval AND @max_calving_interval) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Invalid Calving Interval')
WHERE a.uuid =   @uuid;