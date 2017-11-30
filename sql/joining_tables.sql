drop table if exists ecdc_modeling_table_v0;
create table ecdc_modeling_table_v0 as
(
	select t0.*,
		t2.cm1_before_canc_after_returns,
		t1.value
	from
		(
			select * from ecdc_order_history_financial_agg where case_number > 1 and avg_nsb4ret is not null
		) t0
		left join
		(
			select 
				t1.case_id_key, min(cast(t0.value as varchar(2000))) as value
			from
				research.feat2__customer_all_on_order_agg t0
				left join 
				datamart.dim_order t1
				using(order_id)
			group by t1.case_id_key
		) t1
		using(case_id_key)
		left join
		ecdc_alternative_cm_targets_case t2
		using(case_id_key)
); --372179
select count(*) from ecdc_modeling_table_v0;

select * from ecdc_modeling_table_v0;

