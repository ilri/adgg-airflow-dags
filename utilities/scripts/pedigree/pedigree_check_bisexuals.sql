SET @uuid = '{{ params.uuid }}';

-- Bisexual Check
UPDATE reports.staging_pedigree_data x
JOIN (select a.sire_id animal_id from reports.staging_pedigree_data a
join reports.staging_pedigree_data b on a.sire_id = b.dam_id and a.uuid = @uuid and b.uuid = @uuid) y
ON x.animal_id = y.animal_id SET x.status = 0, x.error = CONCAT(ifnull(x.error,''),' | ','Bisexual(Used A s Sire & Dam)')
WHERE x.uuid = @uuid;




