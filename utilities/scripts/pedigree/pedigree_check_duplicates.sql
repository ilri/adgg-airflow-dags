SET @uuid = '{{ params.uuid }}';

UPDATE reports.staging_pedigree_data a 
JOIN (SELECT tag_id
FROM reports.staging_pedigree_data WHERE uuid = @uuid GROUP BY tag_id HAVING COUNT(tag_id)>1) b
ON a.tag_id = b.tag_id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Duplicate Record')
WHERE a.uuid =   @uuid;


--UPDATE reports.staging_pedigree_data a
--JOIN (SELECT animal_id,farm_id
--FROM reports.staging_pedigree_data WHERE uuid = @uuid GROUP BY animal_id,farm_id HAVING COUNT(animal_id)>1) b
--ON a.animal_id = b.animal_id SET a.status = 0, a.error = CONCAT(ifnull(a.error,''),' | ','Duplicate Record')
--WHERE a.uuid =   @uuid;









