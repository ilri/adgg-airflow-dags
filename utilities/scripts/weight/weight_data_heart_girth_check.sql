SET @uuid = '{{ params.uuid }}';

SELECT graduation_value INTO @calf_age FROM interface_graduation_settings WHERE id = 1 LIMIT 1;


SELECT max_value,min_value INTO @const_calf_girth_max, @const_calf_girth_min FROM interface_limit_parameters WHERE id = 8 LIMIT 1;
SELECT max_value,min_value INTO @const_mature_girth_max, @const_mature_girth_min FROM interface_limit_parameters WHERE id = 5 LIMIT 1;


-- Check for Null Values
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND heart_girth IS NULL AND body_weight IS NULL) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Both Heart Girth & Weight are Null')
WHERE a.uuid =   @uuid;


-- Wild card -> No DOB -> Can't apply calf or mature animal constraints
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND birthdate IS NULL AND heart_girth NOT BETWEEN @const_calf_girth_min AND @const_mature_girth_max AND (heart_girth IS NOT NULL OR heart_girth != 'null')) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Heart Girth Out Of Range')
WHERE a.uuid =   @uuid;

-- calf -> check if girth is within acceptable range
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND heart_girth IS NOT NULL AND age_at_weighing IS NOT NULL AND
age_at_weighing <= @calf_age AND heart_girth NOT BETWEEN @const_calf_girth_min AND @const_calf_girth_max) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Heart Girth Out Of Range(Considered age at weighing)')
WHERE a.uuid =   @uuid;


-- mature animal(cows,bulls, heifers) -> check if girth is within acceptable range
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND heart_girth IS NOT NULL AND age_at_weighing IS NOT NULL AND
age_at_weighing > @calf_age AND heart_girth NOT BETWEEN @const_mature_girth_min AND @const_mature_girth_max) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Heart Girth Out Of Range(Considered age at weighing)')
WHERE a.uuid =   @uuid;









