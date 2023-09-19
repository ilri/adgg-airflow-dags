delete from reports.ldi_herds_by_type;

insert into reports.ldi_herds_by_type(`year`,farm_type,farms)
select year(reg_date)`year`,b.description farm_type,count(a.id) farms
from adgg_uat.core_farm a
join adgg_uat.core_master_list b on a.farm_type = b.value and b.list_type_id = 2
where country_id = 12 and year(reg_date)>=2016
group by `year`,a.farm_type order by `year`;