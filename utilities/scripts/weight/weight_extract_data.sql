SET @uuid = '{{ params.uuid }}';
SET @country = {{ params.country}};
SET @start_date = '{{ params.start_date}}';
SET @end_date = '{{ params.end_date}}';

INSERT INTO reports.staging_weight_data (uuid,registration_date, animal_id, tag_id, main_breed, birthdate, country, region, district, ward, village, weight_date, heart_girth, body_weight,latitude,longitude,sex_id,sex,animal_type,animal_type_id,age_at_weighing)
SELECT
    @uuid,
    ifnull(animal.reg_date,date(animal.created_at)) registration_date,
    animal.id animal_id,
    animal.tag_id,
    ifnull(breeds.label,'Unknown') main_breed,
    animal.birthdate,
    country.`name` AS country,
    region.`name` AS region,
    district.`name` AS district,
    ward.`name` AS ward,
    village.`name` AS village,
    evnt.event_date weight_date,
    JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."137"')) AS heart_girth,
    JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."136"')) AS body_weight,
    animal.latitude,
    animal.longitude,animal.sex,
    sex.label,
    animal_type.label,
    animal.animal_type,
    PERIOD_DIFF(EXTRACT(YEAR_MONTH FROM evnt.event_date), EXTRACT(YEAR_MONTH FROM animal.birthdate))
FROM adgg_uat.core_animal animal
LEFT JOIN adgg_uat.core_country country ON animal.country_id = country.id
LEFT JOIN adgg_uat.country_units region ON animal.region_id = region.id
LEFT JOIN adgg_uat.country_units district ON animal.district_id = district.id
LEFT JOIN adgg_uat.country_units ward ON animal.ward_id = ward.id
LEFT JOIN adgg_uat.country_units village ON animal.village_id = village.id
LEFT JOIN adgg_uat.core_master_list sex ON animal.sex = sex.value AND sex.list_type_id = 3
LEFT JOIN adgg_uat.core_master_list breeds ON animal.main_breed = breeds.value AND breeds.list_type_id = 8
LEFT JOIN adgg_uat.core_master_list animal_type ON animal.animal_type = animal_type.value AND animal_type.list_type_id = 62
LEFT JOIN adgg_uat.core_animal_event evnt ON animal.id = evnt.animal_id
WHERE animal.country_id = @country AND evnt.event_type = 6 AND evnt.event_date BETWEEN @start_date AND @end_date
ORDER BY animal.id;


