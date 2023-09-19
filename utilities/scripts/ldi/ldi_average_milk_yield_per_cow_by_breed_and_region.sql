delete from reports.ldi_average_milk_yield_per_cow_by_breed_and_region;

insert into reports.ldi_average_milk_yield_per_cow_by_breed_and_region(Region,Breed,Average_milk)
SELECT Region,Breed, round(avg(Total_Milk_Volume),2) Average_Milk FROM
(
SELECT
c.name Region,
d.label Breed,
a.animal_id,
avg(replace(JSON_UNQUOTE(JSON_EXTRACT(a.additional_attributes, '$."61"')),'null','')+
replace(JSON_UNQUOTE(JSON_EXTRACT(a.additional_attributes, '$."68"')),'null','')+
replace(JSON_UNQUOTE(JSON_EXTRACT(a.additional_attributes, '$."59"')),'null','')) `Total_Milk_Volume`
from adgg_uat.core_animal_event a
join adgg_uat.core_animal b on a.animal_id = b.id
join adgg_uat.country_units c on b.region_id = c.id
join adgg_uat.core_master_list d on b.main_breed = d.value and d.list_type_id = 8
and a.country_id=12 and a.event_type = 2 and year(a.event_date) >=2016
group by a.animal_id) x
group by Region,Breed;