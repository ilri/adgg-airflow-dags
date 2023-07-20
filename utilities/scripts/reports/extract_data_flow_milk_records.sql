SET @uuid = uuid();
SET @date_from = '{{ params.start_date }}';
SET @date_to = '{{ params.end_date }}';

DELETE FROM reports.rpt_data_flow_milk;

INSERT INTO reports.rpt_data_flow_milk(Country,Milk_date,Total,uuid)
SELECT * FROM (
SELECT country.name,DATE(evnt.created_at),
ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."61"')),'null',''),2) +
ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."68"')),'null',''),2) +
ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."59"')),'null',''),2) total_milk,
@uuid
FROM
 adgg_uat.core_animal animal
 INNER JOIN  adgg_uat.core_animal_event evnt ON evnt.animal_id = animal.id
 INNER JOIN adgg_uat.core_farm  farm ON animal.farm_id = farm.id
 INNER JOIN adgg_uat.core_country country on farm.country_id = country.id
WHERE evnt.event_type = 2 AND evnt.field_agent_id IS NOT NULL AND DATE(evnt.created_at) BETWEEN @date_from AND @date_to
) x WHERE x.total_milk>0






