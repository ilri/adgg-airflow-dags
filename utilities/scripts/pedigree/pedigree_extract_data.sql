SET @uuid = '{{ params.uuid }}';
SET @country = {{ params.country}};


INSERT INTO reports.staging_pedigree_data (country_id, country, region, district, ward, village, farmer_name, farm_id, animal_id, tag_id, original_tag_id, sire_tag_id, sire_id, dam_tag_id, dam_id, sex, sex_id, reg_date, birthdate, main_breed, breed, longitude, latitude,sire_birthdate,dam_birthdate,grand_sire_id,grand_dam_id, uuid) 
SELECT
farm.country_id,
country.name country,
region.name AS Region, 
district.name AS District, 
ward.name AS Ward, 
village.name AS Village,
farm.farmer_name,
animal.farm_id,
animal.id Animal_id,
animal.tag_id,
animal.original_tag_id,
sire.tag_id sire_tag_id,
animal.sire_id,
dam.tag_id dam_tag_id,
animal.dam_id,
sex.label sex,
animal.sex as sex_id,
animal.reg_date,
animal.birthdate,
animal.main_breed,
breed.label breed,
farm.longitude,
farm.latitude,
sire.birthdate,
dam.birthdate,
sire.sire_id,
dam.dam_id,
@uuid
FROM 
 adgg_uat.core_animal animal 
 LEFT JOIN adgg_uat.core_animal sire on animal.sire_id = sire.id 
 LEFT JOIN adgg_uat.core_animal dam on animal.dam_id = dam.id
 LEFT JOIN adgg_uat.core_farm  farm ON animal.farm_id = farm.id
 LEFT JOIN adgg_uat.country_units region ON farm.region_id = region.id 
 LEFT JOIN adgg_uat.country_units district ON farm.district_id = district.id 
 LEFT JOIN adgg_uat.country_units ward ON farm.ward_id = ward.id 
 LEFT JOIN adgg_uat.country_units village ON farm.village_id = village.id 
 LEFT JOIN adgg_uat.core_master_list breed ON breed.value = animal.main_breed  AND breed.list_type_id = 8
 LEFT JOIN adgg_uat.core_master_list sex ON sex.value = animal.sex  AND sex.list_type_id = 3
 LEFT JOIN adgg_uat.core_country country on farm.country_id = country.id
WHERE farm.country_id = @country;
