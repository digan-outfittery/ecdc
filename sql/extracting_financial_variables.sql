drop table if exists ecdc_case_financal_metrics;
create table ecdc_case_financal_metrics as
(
	select *,
		row_number() over (partition by customer_id order by date_observed asc) as case_number /*,
		
		
		pg_catalog.lag(nsb4ret_in_eur) over (partition by case_id_key order by date_observed) as prev_nsb4ret,
		pg_catalog.lag(cogb4ret) over (partition by case_id_key order by date_observed) as prev_cogb4ret,
		pg_catalog.lag(nsexp_in_eur) over (partition by case_id_key order by date_observed) as prev_nsexp,
		pg_catalog.lag(cogexp) over (partition by case_id_key order by date_observed) as prev_cog_exp,
		pg_catalog.lag(cm1) over (partition by case_id_key order by date_observed) as prev_cm1,
		pg_catalog.lag(cm2) over (partition by case_id_key order by date_observed) as prev_cm2,
		pg_catalog.lag(cm3) over (partition by case_id_key order by date_observed) as prev_cm3,
		
		pg_catalog.lag(t0.netdisc_in_eur) over (partition by t0.case_id_key order by t0.date_observed) as prev_netdisc,
		pg_catalog.lag(t0.netgoodwill_in_eur) over (partition by t0.case_id_key order by t0.date_observed) as prev_netgoodwill,
		pg_catalog.lag(t0.paid_voucher_in_eur) over (partition by t0.case_id_key order by t0.date_observed) as prev_paid_voucher*/
		
		
	from
	(
		select case_id_key, customer_id,
			min(date_observed) as date_observed,
			
			
			sum(nsb4ret_in_eur) as nsb4ret_in_eur,
			sum(cogb4ret) as cogb4ret,
			sum(nsexp_in_eur) as nsexp_in_eur,
			sum(cogexp) as cogexp,
			sum(cm1) as cm1,
			sum(cm2) as cm2,
			sum(cm3) as cm3,
			
			sum(netdisc_in_eur) as netdisc_in_eur,
			sum(netgoodwill_in_eur) as netgoodwill_in_eur,
			sum(paid_voucher_in_eur) as paid_voucher_in_eur
			
		from 
		(
			select distinct t0.*,
				t1.customer_id, t1.case_id_key,
				t1.date_order_created,
				t1.date_order_invoiced,
				t1.date_order_completed,
				t1.date_order_shipped,
				t1.date_order_returned,
				t1.date_order_returned_online,
			    coalesce(t1.date_order_returned, t1.date_order_shipped + interval '40 days') as date_observed,
			    
			    t2.cm1,
			    t2.cm2,
			    t2.cm3
				
			from 
				datamart.fact_order t0 left join
				datamart.dim_order t1 using(order_id_key) left join
				(select * from datamart.fact_contribution_margin where version_code = 'FACT') t2 using(order_id_key)
			where
				t1.is_real_order = 1
		) t	
		
		group by case_id_key, customer_id
		
	) t
); --874374 rows
select count(*) from ecdc_case_financal_metrics;

select * from ecdc_case_financal_metrics;

				


drop table if exists ecdc_order_history_financial_agg;
create table ecdc_order_history_financial_agg as
(
	select t0.customer_id, t0.case_id_key, t0.date_observed,
		t0.case_number,
		extract(epoch from t0.date_observed - max(t1.date_observed)) / (60*60*24) as days_since_last_order,
		avg(t1.nsb4ret_in_eur) as avg_nsb4ret,
		
		avg(t1.cogb4ret) as avg_cogb4ret, 
		avg(t1.nsexp_in_eur) as avg_nsexp,
		avg(t1.cogexp) as avg_cog_exp,
		avg(t1.cm1) as avg_cm1,
		avg(t1.cm2) as avg_cm2,
		avg(t1.cm3) as avg_cm3,
		
		sum(t1.netdisc_in_eur)/nullif(sum(t0.nsb4ret_in_eur),0) as overall_discount_perc,
		sum(t1.netgoodwill_in_eur)/nullif(sum(t0.nsb4ret_in_eur),0) as overall_goodwill_perc,
		sum(t1.paid_voucher_in_eur)/nullif(sum(t0.nsb4ret_in_eur),0) as overall_voucher_perc
		
	from
		ecdc_case_financal_metrics t0 left join 
		ecdc_case_financal_metrics t1 on
		t0.customer_id = t1.customer_id and
		t0.date_observed > t1.date_observed
		
	group by
		t0.case_id_key, t0.date_observed, t0.customer_id, t0.case_number

); --874374 rows

select * from ecdc_order_history_financial_agg;
select count(*) from ecdc_order_history_financial_agg;


select * from ecdc_order_history_financial_agg where customer_id is not null and date_observed is not null and (case_number > 1 and avg_nsb4ret is null);

select * from ecdc_case_financal_metrics where customer_id = 86878255;

select * from datamart.dim_order where customer_id = 86878255


select * from ecdc_case_financal_metrics where case_id_key = 28400;

