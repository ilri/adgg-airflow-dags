SET @uuid = '{{ params.uuid }}';

SELECT graduation_value INTO @calf_age FROM interface_graduation_settings WHERE id = 1 LIMIT 1;


SELECT max_value,min_value INTO @const_mature_weight_max, @const_mature_weight_min FROM interface_limit_parameters WHERE id = 6 LIMIT 1;
SELECT max_value,min_value INTO @const_calf_weight_max, @const_calf_weight_min FROM interface_limit_parameters WHERE id = 7 LIMIT 1;


-- Wild card -> No DOB -> Can't apply calf or mature animal constraints
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND birthdate IS NULL AND body_weight NOT BETWEEN @const_calf_weight_min 
AND @const_mature_weight_max AND (body_weight IS NOT NULL OR body_weight != 'null')) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Body Weight Out Of Range')
WHERE a.uuid =   @uuid;

-- calf -> check if weight is within acceptable range
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND body_weight IS NOT NULL AND age_at_weighing IS NOT NULL AND
age_at_weighing <= @calf_age AND body_weight NOT BETWEEN @const_calf_weight_min AND @const_calf_weight_max) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Body Weight Out Of Range(Considered age at weighing)')
WHERE a.uuid =   @uuid;


-- mature animal(cows,bulls, heifers) -> check if weight is within acceptable range
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND body_weight IS NOT NULL AND age_at_weighing IS NOT NULL AND
age_at_weighing > @calf_age AND body_weight NOT BETWEEN @const_mature_weight_min AND @const_mature_weight_max) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Body Weight Out Of Range(Considered age at weighing)')
WHERE a.uuid =   @uuid;









