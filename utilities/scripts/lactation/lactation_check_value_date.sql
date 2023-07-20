SET @uuid = '{{ params.uuid }}';

-- Recorded Before DOB
UPDATE reports.staging_calving_data a
JOIN (SELECT id FROM reports.staging_calving_data WHERE uuid = @uuid AND birthdate > calving_date AND calving_date IS NOT NULL AND birthdate IS NOT NULL) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Calved Before Birth')
WHERE a.uuid = @uuid;


-- Future Record
UPDATE reports.staging_calving_data a
JOIN (SELECT id FROM reports.staging_calving_data WHERE uuid = @uuid AND calving_date > CURDATE()  AND calving_date IS NOT NULL) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Future Calving Date')
WHERE a.uuid =   @uuid;






