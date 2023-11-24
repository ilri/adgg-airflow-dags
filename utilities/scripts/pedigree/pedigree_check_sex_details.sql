SET @uuid = '{{ params.uuid }}';

-- check for null values in the sex column
UPDATE reports.staging_pedigree_data a
JOIN (SELECT id FROM reports.staging_pedigree_data WHERE uuid = @uuid AND sex is null) b
ON a.id = b.id SET a.status = 0, a.warning = CONCAT(ifnull(a.warning,''),' | ','No or Estimated Sex Details')
WHERE a.uuid = @uuid;

-- Derive Sex (Female) -> records should have events data associated with female animals
UPDATE reports.staging_pedigree_data a
JOIN (SELECT x.id,x.animal_id FROM reports.staging_pedigree_data x join adgg_uat.core_animal_event y
ON x.animal_id = y.animal_id and x.country_id = y.country_id
WHERE x.uuid = @uuid AND x.sex is null and y.event_type IN (1,2,3,4,5)
) b
ON a.id = b.id SET a.sex = 'Female'
WHERE a.uuid = @uuid;


-- Derive Sex (Male) -> Animals used as a sire
UPDATE reports.staging_pedigree_data a
JOIN reports.staging_pedigree_data b on a.id= b.sire_id and a.country_id = b.country_id
SET a.sex= 'Male'
WHERE a.uuid = @uuid AND b.uuid = @uuid AND a.sex is null;

-- Derive Sex (Female) -> Animals used as a dam
UPDATE reports.staging_pedigree_data a
JOIN reports.staging_pedigree_data b on a.id= b.dam_id and a.country_id = b.country_id
SET a.sex= 'Female'
WHERE a.uuid = @uuid AND b.uuid = @uuid AND a.sex is null;












