SET @uuid = '{{ params.uuid }}';

-- check for null values in the sex column
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM reports.staging_pedigree_data WHERE uuid = @uuid AND sex is null) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','No Sex Details')
WHERE a.uuid = @uuid;







