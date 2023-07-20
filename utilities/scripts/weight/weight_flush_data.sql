SET @uuid = '{{ params.uuid }}';

DELETE FROM reports.staging_weight_data WHERE uuid = @uuid;