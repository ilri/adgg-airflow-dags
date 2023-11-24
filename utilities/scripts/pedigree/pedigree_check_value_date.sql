SET @uuid = '{{ params.uuid }}';

-- Recorded Before DOB
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM reports.staging_pedigree_data WHERE uuid = @uuid AND birthdate > reg_date AND birthdate IS NOT NULL AND reg_date IS NOT NULL) b
ON a.id = b.id SET a.status = 0, a.warning = CONCAT(ifnull(a.warning,''),' | ','Registered Before Birth')
WHERE a.uuid = @uuid;









