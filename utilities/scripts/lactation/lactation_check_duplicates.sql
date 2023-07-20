SET @uuid = '{{ params.uuid }}';

UPDATE reports.staging_calving_data a 
JOIN (SELECT animal_id,calving_date 
FROM reports.staging_calving_data WHERE uuid = @uuid GROUP BY animal_id,calving_date HAVING COUNT(animal_id)>1) b 
ON a.animal_id = b.animal_id AND a.calving_date=b.calving_date SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Duplicate Record')
WHERE a.uuid =   @uuid;
