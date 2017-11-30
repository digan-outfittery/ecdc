drop table if exists ecdc_alternative_cm_targets_order;
CREATE TABLE ecdc_alternative_cm_targets_order AS
(
	SELECT t2.case_id_key, t0.*, t1.cm1, t1.cm2, t1.cm3,
		case when nsb4canc_in_eur > 0 then cm1/nsb4canc_in_eur else null end AS cm1_ratio,
		nsb4canc_in_eur - cogb4canc AS cm1_before_canc,
		(nsb4canc_in_eur - cogb4canc) - (nret_in_eur - cogret) AS cm1_before_canc_after_returns,
		(nsb4canc_in_eur - cogb4canc) - (nret_in_eur - cogret) - (estnret_in_eur - estcogret) AS cm1_before_canc_after_exp_returns
	FROM
		datamart.fact_order t0 left join
		(select * from datamart.fact_contribution_margin where version_code = 'FACT') t1
		using(order_id) left join
		datamart.dim_order t2
		using(order_id)
	where
		t2.is_real_order = 1
); --999466 rows

select count(*) from ecdc_alternative_cm_targets_order;


drop table if exists ecdc_alternative_cm_targets_case;
create table ecdc_alternative_cm_targets_case as
(
	select case_id_key, 
		sum(cm1) as cm1,
		sum(cm2) as cm2,
		sum(cm3) as cm3,
		
		sum(cm1)/nullif(sum(nsb4canc_in_eur),0) as cm1_ratio,
		
		sum(cm1_before_canc) as cm1_before_canc,
		sum(cm1_before_canc_after_returns) as cm1_before_canc_after_returns,
		sum(cm1_before_canc_after_exp_returns) as cm1_before_canc_after_exp_returns
		
	from
		ecdc_alternative_cm_targets_order
		
	group by case_id_key
); --874374 rows