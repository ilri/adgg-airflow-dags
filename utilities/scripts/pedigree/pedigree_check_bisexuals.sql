SET @uuid = '{{ params.uuid }}';

-- Bisexual Check
UPDATE reports.staging_pedigree_data x
JOIN (select a.sire_id animal_id from reports.staging_pedigree_data a
join adgg_uat.core_animal b on a.sire_id = b.dam_id and a.country_id = b.country_id and a.uuid = @uuid and b.uuid = @uuid) y
ON x.animal_id = y.animal_id SET x.status = 0, x.error = CONCAT(ifnull(x.error,''),' | ','Bisexual(Sire used as Dam)')
WHERE x.uuid = @uuid;







