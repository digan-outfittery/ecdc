select * from 
(
	select t0.order_id, t0.cm1 as fact_cm1, t1.cm1 as model_cm1
	from
	(select * from datamart.fact_contribution_margin where version_code = 'FACT') t0 
		left join
	(select * from datamart.fact_contribution_margin where version_code = 'MODEL_V3') t1
	on t0.order_id = t1.order_id
) t
where fact_cm1 <> model_cm1;
--All equal

select count(*), version_code 
from datamart.fact_contribution_margin
group by version_code;
--MODEL_V3: 810,326
--FACT: 986,299
			