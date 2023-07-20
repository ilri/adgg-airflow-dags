SET @uuid = uuid();
SET @date_from = '{{ params.start_date }}';
SET @date_to = '{{ params.end_date }}';

DELETE FROM reports.rpt_data_flow_weight;
INSERT INTO reports.rpt_data_flow_weight(Country,Weight_date,Total,uuid)
SELECT country.name country,DATE(evnt.created_at),
JSON_UNQUOTE(JSON_EXTRACT(evnt.additional_attributes, '$."136"'))  weight,
@uuid
FROM adgg_uat.core_animal_event evnt
INNER JOIN adgg_uat.core_country country on evnt.country_id = country.id
WHERE evnt.event_type = 6 AND evnt.field_agent_id IS NOT NULL
and DATE(evnt.created_at) BETWEEN @date_from AND @date_to;






