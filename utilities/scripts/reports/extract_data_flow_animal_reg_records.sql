SET @uuid = uuid();
SET @date_from = '{{ params.start_date }}';
SET @date_to = '{{ params.end_date }}';

DELETE FROM reports.rpt_data_flow_animal_reg;
INSERT INTO reports.rpt_data_flow_animal_reg(Country,Animal_type,total,uuid)
SELECT b.name, c.label,COUNT(a.id),@uuid FROM adgg_uat.core_animal a
INNER JOIN adgg_uat.core_country b ON b.id = a.country_id
INNER JOIN adgg_uat.core_master_list c ON c.value = a.animal_type AND c.list_type_id = 62
WHERE DATE(a.created_at)  BETWEEN @date_from AND @date_to AND a.created_by IS NOT NULL
GROUP BY a.country_id, a.animal_type;





