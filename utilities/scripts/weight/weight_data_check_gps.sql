SET @uuid = '{{ params.uuid }}';

-- GPS out of Range
UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND longitude IS NOT NULL AND latitude IS NOT NULL AND ((longitude NOT BETWEEN -180 AND 180) OR (latitude NOT BETWEEN -180 AND 180))) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','GPS Out Of Range')
WHERE a.uuid = @uuid;


UPDATE reports.staging_weight_data a
JOIN (SELECT id FROM reports.staging_weight_data WHERE uuid = @uuid AND (longitude IS NULL OR latitude IS NULL)) b
ON a.id = b.id SET a.status = 0, a.comments = CONCAT(ifnull(a.comments,''),' | ','No GPS Details')
WHERE a.uuid = @uuid;