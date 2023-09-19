delete from reports.ldi_weekly_ai_services_by_region_and_technician;

insert into reports.ldi_weekly_ai_services_by_region_and_technician(region,technician,`year`,`week`,inseminations)
select c.name region,b.name as technician,YEAR(a.event_date) AS `year`,WEEK(a.event_date) AS `week`,
count(a.id) inseminations
from adgg_uat.core_animal_event a join adgg_uat.auth_users b on a.field_agent_id =b.id
join adgg_uat.country_units c on a.region_id = c.id
and a.event_type = 3 and a.country_id = 12 and year(a.event_date)>= 2016
group by region,technician,`year`,`week`;