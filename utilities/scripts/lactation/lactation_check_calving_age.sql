SET @uuid = '{{ params.uuid }}';

SELECT ifnull(min_value,18)*30,ifnull(max_value,48)*30 INTO @min_calving_age,@max_calving_age FROM interface_limit_parameters where category = 'age_at_first_calving' LIMIT 1;


-- Calving Age
UPDATE reports.staging_calving_data a
JOIN (SELECT id FROM reports.staging_calving_data WHERE uuid = @uuid AND calving_age IS NOT NULL AND calving_age < @min_calving_age) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Invalid Calving Age')
WHERE a.uuid =   @uuid;