SET @uuid = '{{ params.uuid }}';


-- Check Dams
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sex = 1 AND id IN (SELECT dam_id  FROM adgg_uat.core_animal)) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Male Animal Used As A Dam')
WHERE a.uuid = @uuid;


-- Check Sires
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sex = 2 AND id IN (SELECT sire_id  FROM adgg_uat.core_animal)) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Female Animal Used As A Sire')
WHERE a.uuid = @uuid;


-- Own Parent
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM adgg_uat.core_animal WHERE uuid = @uuid AND sire_id = id or dam_id = id) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Own Parent')
WHERE a.uuid = @uuid;







