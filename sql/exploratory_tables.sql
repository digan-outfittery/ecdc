drop table if exists ecdc_alternative_cm_targets;
CREATE TABLE ecdc_alternative_cm_targets AS
(
	SELECT t0.*, t1.cm1, t1.cm2, t1.cm3,
		case when nsb4canc_in_eur > 0 then cm1/nsb4canc_in_eur else null end AS cm1_ratio,
		nsb4canc_in_eur - cogb4canc AS cm1_before_canc,
		(nsb4canc_in_eur - cogb4canc) - (nret_in_eur - cogret) AS cm1_before_canc_after_returns,
		(nsb4canc_in_eur - cogb4canc) - (nret_in_eur - cogret) - (estnret_in_eur - estcogret) AS cm1_before_canc_after_exp_returns
	FROM
		datamart.fact_order t0 left join
		(select * from datamart.fact_contribution_margin where version_code = 'FACT') t1
		using(order_id)
);


select * from ecdc_alternative_cm_targets where cm1_before_canc_after_returns = 0;