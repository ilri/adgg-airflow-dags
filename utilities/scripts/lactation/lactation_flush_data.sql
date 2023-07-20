SET @uuid = '{{ params.uuid }}';

DELETE FROM reports.staging_calving_data WHERE uuid = @uuid;