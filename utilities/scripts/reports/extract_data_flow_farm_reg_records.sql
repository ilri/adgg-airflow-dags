SET @uuid = uuid();
SET @date_from = '{{ params.start_date }}';
SET @date_to = '{{ params.end_date }}';

DELETE FROM reports.rpt_data_flow_farm_reg;
INSERT INTO reports.rpt_data_flow_farm_reg(Country,Farm_type,Total,uuid)
SELECT b.name, c.label,COUNT(a.id),@uuid FROM adgg_uat.core_farm a
INNER JOIN adgg_uat.core_country b ON b.id = a.country_id
INNER JOIN adgg_uat.core_master_list c ON c.value = a.farm_type AND c.list_type_id = 2
WHERE DATE(a.created_at)  BETWEEN @date_from AND @date_to AND country_id IS NOT NULL AND a.field_agent_id IS NOT NULL
GROUP BY a.country_id, a.farm_type;





