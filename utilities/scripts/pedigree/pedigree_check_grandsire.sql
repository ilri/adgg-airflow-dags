SET @uuid = '{{ params.uuid }}';

-- Progeny Is Its Own Grand Sire
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM reports.staging_pedigree_data WHERE uuid = @uuid AND animal_id = grand_sire_id) b
ON a.id = b.id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Progeny Is Its Own Grand Sire')
WHERE a.uuid = @uuid;







