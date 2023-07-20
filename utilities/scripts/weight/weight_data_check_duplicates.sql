SET @uuid = '{{ params.uuid }}';

UPDATE reports.staging_weight_data a 
JOIN (SELECT animal_id,weight_date 
FROM reports.staging_weight_data WHERE uuid = @uuid GROUP BY animal_id,weight_date HAVING COUNT(animal_id)>1) b 
ON a.animal_id = b.animal_id AND a.weight_date=b.weight_date SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Duplicate Record')
WHERE a.uuid =   @uuid;

