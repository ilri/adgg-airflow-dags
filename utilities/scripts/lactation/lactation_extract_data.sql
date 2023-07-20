SET @uuid = '{{ params.uuid }}';
SET @country = {{ params.country}};
SET @start_date = '{{ params.start_date}}';
SET @end_date = '{{ params.end_date}}';

INSERT INTO reports.staging_calving_data (report_run_date,farm_name, country, region, district, ward, village, longitude, latitude, animal_id, tag_id, animal_registration_date, main_breed, birthdate, animal_sex, animal_type_id, animal_type,lactation_id, calving_date, calving_age, previous_calving_date,subsequent_calving_date, calving_interval, parity, uuid) 
SELECT
    CURDATE(),	
	farm.farmer_name farm_name,
	country.`name` AS country, 
	region.`name` AS region,
	district.`name` AS district,
	ward.`name` AS ward,
	village.`name` AS village,
	farm.longitude,
	farm.latitude,	
	animal.id animal_id,  
	animal.tag_id, 
	ifnull(animal.reg_date,date(animal.created_at)) animal_registration_date,
	ifnull(breeds.label,'Unknown') main_breed,	
	animal.birthdate,    
	sex.label animal_sex,  
	animal.animal_type,	
	animal_type.label animal_type,
	evnt.id lactation_id,
	evnt.event_date calving_date,
    datediff(evnt.event_date,animal.birthdate) calving_age,
	LAG(evnt.event_date,1) OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date asc) previous_calving_date,
	LAG(evnt.event_date,1) OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date desc) subsequent_calving_date,
	DATEDIFF(evnt.event_date,LAG(evnt.event_date,1) OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date )) calving_interval,
	RANK() OVER (PARTITION BY evnt.animal_id ORDER BY evnt.event_date ) parity,
	@uuid
FROM core_animal animal
JOIN core_farm farm  ON animal.farm_id = farm.id
LEFT JOIN core_country country ON farm.country_id = country.id
LEFT JOIN country_units region ON farm.region_id = region.id
LEFT JOIN country_units district ON farm.district_id = district.id
LEFT JOIN country_units ward ON farm.ward_id = ward.id
LEFT JOIN country_units village ON farm.village_id = village.id
LEFT JOIN core_master_list sex ON animal.sex = sex.value AND sex.list_type_id = 3
LEFT JOIN core_master_list breeds on animal.main_breed = breeds.value AND breeds.list_type_id = 8
LEFT JOIN core_master_list animal_type on animal.animal_type  = animal_type.value AND animal_type.list_type_id = 62
LEFT JOIN core_animal_event evnt ON animal.id = evnt.animal_id
WHERE farm.country_id = @country AND evnt.event_type = 1 AND animal.animal_type !=6 AND evnt.event_date BETWEEN @start_date AND @end_date
ORDER BY animal.id;