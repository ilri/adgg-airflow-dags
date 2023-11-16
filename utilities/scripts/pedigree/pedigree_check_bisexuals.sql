SET @uuid = '{{ params.uuid }}';

-- Bisexual Check
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND id IN (SELECT sire_id FROM adgg_uat.core_animal  WHERE sire_id IS NOT NULL)
AND id IN (SELECT dam_id FROM adgg_uat.core_animal  WHERE dam_id IS NOT NULL)) b
ON a.id = b.id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Bisexual(Used As Sire & Dam)')
WHERE a.uuid = @uuid;




