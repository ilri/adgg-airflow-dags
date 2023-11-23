SET @uuid = '{{ params.uuid }}';

-- age comparison

CREATE TEMPORARY TABLE IF NOT EXISTS reports.temp_sire_dob_check (
    id int,
    uuid varchar(200)
);

INSERT INTO reports.temp_sire_dob_check(id,uuid)
SELECT id,uuid FROM reports.staging_pedigree_data WHERE uuid = @uuid AND birthdate IS NOT NULL AND sire_birthdate IS NOT NULL AND birthdate < sire_birthdate;

UPDATE reports.staging_pedigree_data a JOIN reports.temp_sire_dob_check b ON a.id = b.id
SET a.status = 0, a.warning = CONCAT(ifnull(a.warning,''),' | ','Animal Older Than Its Sire')
WHERE a.uuid = @uuid AND b.uuid = @uuid;

DELETE FROM reports.temp_sire_dob_check WHERE uuid = @uuid;








