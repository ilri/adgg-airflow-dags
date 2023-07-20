SET @uuid = '{{ params.uuid }}';

-- Bisexual Check
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM reports.staging_pedigree_data WHERE uuid = @uuid AND birthdate IS NOT NULL AND sire_birthdate IS NOT NULL AND birthdate < sire_birthdate) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Animal Older Than Its Sire')
WHERE a.uuid = @uuid;








