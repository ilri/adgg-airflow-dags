SET @uuid = '{{ params.uuid }}';

-- Bisexual Check
UPDATE reports.staging_pedigree_data x
JOIN (
SELECT distinct a.sire_id animal_id FROM adgg_uat.core_animal a JOIN adgg_uat.core_animal b ON a.sire_id = b.dam_id
UNION
SELECT distinct a.dam_id animal_id  FROM adgg_uat.core_animal a JOIN adgg_uat.core_animal b ON a.dam_id = b.sire_id
) y
ON x.animal_id = y.animal_id SET x.status = 0, x.error = CONCAT(ifnull(x.error,''),' | ','Bisexual(Used As Dam and Sire)')
WHERE x.uuid = @uuid;








