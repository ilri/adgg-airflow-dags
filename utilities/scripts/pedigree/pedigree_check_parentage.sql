SET @uuid = '{{ params.uuid }}';


-- Check Dams
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sex = 1 AND id IN (SELECT dam_id  FROM adgg_uat.core_animal)) b
ON a.id = b.id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Male Animal Used As A Dam')
WHERE a.uuid = @uuid;

UPDATE reports.staging_pedigree_data a
SET a.status = 0, a.warning = CONCAT(ifnull(a.warning,''),' | ','Dam Details Not Found')
WHERE a.uuid = @uuid and a.dam_tag_id is not null and a.dam_id is null;

-- Check Sires
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sex = 2 AND id IN (SELECT sire_id  FROM adgg_uat.core_animal)) b
ON a.id = b.id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Female Animal Used As A Sire')
WHERE a.uuid = @uuid;

UPDATE reports.staging_pedigree_data a
SET a.status = 0, a.warning = CONCAT(ifnull(a.warning,''),' | ','Sire Details Not Found')
WHERE a.uuid = @uuid and a.sire_tag_id is not null and a.sire_id is null


-- Own Parent
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sire_id = id or dam_id = id) b
ON a.id = b.id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Own Parent')
WHERE a.uuid = @uuid;







