SET @uuid = uuid();
SET @date_from = '{{ params.start_date }}';
SET @date_to = '{{ params.end_date }}';

DELETE FROM reports.rpt_data_flow;

INSERT INTO reports.rpt_data_flow(country,enumerator,event,total,uuid)
SELECT
	core_country.name,
    auth_users.name,
	events.description event_type,
    count(x.id),
    @uuid
FROM (
    #events data excluding milk
    SELECT
        core_animal_event.id,
        core_animal_event.country_id,
        ifnull(core_animal_event.field_agent_id,core_animal_event.created_by) user_id,
        core_animal_event.event_type
    FROM adgg_uat.core_animal_event
    WHERE  date(core_animal_event.created_at) BETWEEN @date_from AND @date_to	AND  event_type not in (2,18)
	UNION #events data milk only
    SELECT id,country_id,user_id,event_type FROM (
    SELECT
        core_animal_event.id,
        core_animal_event.country_id,
        ifnull(core_animal_event.field_agent_id,core_animal_event.created_by) user_id,
        core_animal_event.event_type,
        ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."61"')),'null',''),2) +
        ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."68"')),'null',''),2) +
        ROUND(replace(JSON_UNQUOTE(JSON_EXTRACT(core_animal_event.additional_attributes, '$."59"')),'null',''),2) total_milk
    FROM adgg_uat.core_animal_event
    WHERE  date(core_animal_event.created_at) BETWEEN @date_from AND @date_to	AND  event_type in (2) ) x where total_milk >0
    UNION #animal data
    SELECT
        core_animal.id,
        core_animal.country_id,
        core_animal.created_by pra,
        100 event_type
    FROM adgg_uat.core_animal
    WHERE date(core_animal.created_at) BETWEEN @date_from AND @date_to AND core_animal.country_id IS NOT NULL AND core_animal.created_by IS NOT NULL
    UNION
    #farm data
    SELECT
    core_farm.id,
    core_farm.country_id,
    ifnull(core_farm.field_agent_id,core_farm.created_by) user_id,
    101 event_type
    FROM adgg_uat.core_farm
    WHERE date(core_farm.created_at)  BETWEEN @date_from AND @date_to and core_farm.country_id IS NOT NULL
    ) x
INNER JOIN adgg_uat.core_events_type events ON events.event_id = x.event_type
INNER JOIN adgg_uat.auth_users ON x.user_id = auth_users.id
INNER JOIN adgg_uat.core_country ON x.country_id = core_country.id
GROUP BY x.country_id, x.user_id, x.event_type;





