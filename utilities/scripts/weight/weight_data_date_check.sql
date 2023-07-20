SET @uuid = '{{ params.uuid }}';


-- Recorded Before DOB
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND birthdate > weight_date AND birthdate IS NOT NULL AND weight_date IS NOT NULL) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Weight Recorded Before Animal Was Born')
WHERE a.uuid =   @uuid;


-- Future Record
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND weight_date > CURDATE()  AND weight_date IS NOT NULL) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Weight Date Is In The Future')
WHERE a.uuid =   @uuid;



