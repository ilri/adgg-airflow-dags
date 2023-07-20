SET @uuid = '{{ params.uuid }}';


-- Animal Type
UPDATE reports.staging_calving_data a
JOIN (SELECT id FROM reports.staging_calving_data WHERE uuid = @uuid AND animal_type_id IS NOT NULL AND animal_type_id NOT IN (1,2)) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','Invalid Animal Type')
WHERE a.uuid =   @uuid;