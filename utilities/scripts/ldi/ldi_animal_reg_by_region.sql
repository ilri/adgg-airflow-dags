delete from reports.ldi_animal_reg_by_region;

insert into reports.ldi_animal_reg_by_region(region,animals)
select c.name region,count(a.id) animals
from adgg_uat.core_animal a
join adgg_uat.country_units c on a.region_id = c.id and
a.country_id = 12 and year(a.reg_date)>= 2016
group by region;