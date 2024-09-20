#!/bin/bash
source ~/.bash_profile

start_date=$1
end_date=$2

start_pt=`date -d"-0 day $start_date" '+%Y%m%d'`
end_pt=`date -d"-0 day $end_date" '+%Y%m%d'`

echo "start date: ${start_date}"
echo "end date: ${end_pt}"
echo "start pt: ${start_pt}"
echo "end pt: ${end_pt}"


hive -e "
drop table if exsist manhattan_test.user_demand_model_account_status_stat_feature_${start_pt}_${end_pt};
create table manhattan_test.user_demand_model_account_status_stat_feature_${start_pt}_${end_pt} as
 
select
    t0.*,
    
    case when current_drip_credit_line between 0 and 4999 then 1 else 0 end as credit_line_level_1,
    case when current_drip_credit_line between 5000 and 9999 then 1 else 0 end as credit_line_level_2,
    case when current_drip_credit_line between 10000 and 49999 then 1 else 0 end as credit_line_level_3,
    case when current_drip_credit_line between 50000 and 99999 then 1 else 0 end as credit_line_level_4,
    case when current_drip_credit_line between 100000 and 199999 then 1 else 0 end as credit_line_level_5,
    case when current_drip_credit_line >= 200000 then 1 else 0 end as credit_line_level_6,

    case when current_drip_interest_rate between 0 and 3.287 then 1 else 0 end as current_drip_interest_rate_level_1,
    case when current_drip_interest_rate between 3.288 and 4.383 then 1 else 0 end as current_drip_interest_rate_level_2,
    case when current_drip_interest_rate between 4.384 and 4.931 then 1 else 0 end as current_drip_interest_rate_level_3,
    case when current_drip_interest_rate between 4.932 and 5.476 then 1 else 0 end as current_drip_interest_rate_level_4,
    case when current_drip_interest_rate between 5.477 and 6.575 then 1 else 0 end as current_drip_interest_rate_level_5,
    case when current_drip_interest_rate between 6.576 and 8.219 then 1 else 0 end as current_drip_interest_rate_level_6,
    case when current_drip_interest_rate between 8.220 and 9.863 then 1 else 0 end as current_drip_interest_rate_level_7,
    case when current_drip_interest_rate >= 9.864 then 1 else 0 end as current_drip_interest_rate_level_8,

    case when current_drip_credit_line_balance between 0 and 4999 then 1 else 0 end as current_drip_credit_line_balance_level_1,
    case when current_drip_credit_line_balance between 5000 and 9999 then 1 else 0 end as current_drip_credit_line_balance_level_2,
    case when current_drip_credit_line_balance between 10000 and 49999 then 1 else 0 end as current_drip_credit_line_balance_level_3,
    case when current_drip_credit_line_balance between 50000 and 99999 then 1 else 0 end as current_drip_credit_line_balance_level_4,
    case when current_drip_credit_line_balance between 100000 and 199999 then 1 else 0 end as current_drip_credit_line_balance_level_5,
    case when current_drip_credit_line_balance >= 200000 then 1 else 0 end as current_drip_credit_line_balance_level_6,

    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) between 0 and 1 then 1 else 0 end as last_credit_success_start_time_level_1,
    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) between 2 and 3 then 1 else 0 end as last_credit_success_start_time_level_2,
    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) between 4 and 6 then 1 else 0 end as last_credit_success_start_time_level_3,
    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) between 7 and 12 then 1 else 0 end as last_credit_success_start_time_level_4,
    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) between 13 and 24 then 1 else 0 end as last_credit_success_start_time_level_5,
    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) >= 25 then 1 else 0 end as last_credit_success_end_time_level_6,

    case when round(nvl(plan_amount,0),0) = 0 then 1 else 0 end as plan_amount_level_1,
    case when round(nvl(plan_amount,0),0) between 1 and 100 then 1 else 0 end as plan_amount_level_2,
    case when round(nvl(plan_amount,0),0) between 101 and 500 then 1 else 0 end as plan_amount_level_3,
    case when round(nvl(plan_amount,0),0) between 501 and 1000 then 1 else 0 end as plan_amount_level_4,
    case when round(nvl(plan_amount,0),0) between 1001 and 5000 then 1 else 0 end as plan_amount_level_5,
    case when round(nvl(plan_amount,0),0) between 5001 and 10000 then 1 else 0 end as plan_amount_level_6,
    case when round(nvl(plan_amount,0),0) between 10001 and 50000 then 1 else 0 end as plan_amount_level_7,
    case when round(nvl(plan_amount,0),0) between 50001 and 100000 then 1 else 0 end as plan_amount_level_8,
    case when round(nvl(plan_amount,0),0) >= 100001 then 1 else 0 end as plan_amount_level_9,

    case when nvl(duration_num, 0) = 0 then 1 else 0 end as duration_num_level_1,
    case when nvl(duration_num, 0) between 1 and 3 then 1 else 0 end as duration_num_level_2,
    case when nvl(duration_num, 0) between 4 and 7 then 1 else 0 end as duration_num_level_3,
    case when nvl(duration_num, 0) between 8 and 10 then 1 else 0 end as duration_num_level_4,
    case when nvl(duration_num, 0) between 11 and 15 then 1 else 0 end as duration_num_level_5,
    case when nvl(duration_num, 0) >= 16 then 1 else 0 end as duration_num_level_6,

    case when nvl(current_valid_coupon_num,0) = 0 then 1 else 0 end as current_valid_coupon_num_level_1,
    case when nvl(current_valid_coupon_num,0) between 1 and 5 then 1 else 0 end as current_valid_coupon_num_level_2,
    case when nvl(current_valid_coupon_num,0) between 6 and 10 then 1 else 0 end as current_valid_coupon_num_level_3,
    case when nvl(current_valid_coupon_num,0) >= 11 then 1 else 0 end as current_valid_coupon_num_level_4
from
    (
        select uid, funds_channel_id, tel, label, server_date
        from manhattan_test.dsd_user_loan_demand_samples_label_${start_pt}_${end_pt}
        where funds_channel_id is not null
    ) t0 

    left outer join 
    (
        select
            uid, concat_ws('-', year, month, day) as pt, 
            current_drip_credit_line,
            current_drip_interest_rate,
            current_drip_credit_line_balance,
            last_credit_success_start_time
        from 
            riskmanage_dm.feature_individual_manhattan_drip_credit
        where
            concat_ws('-', year, month, day) between date_add('${start_pt}',-1) and '${end_pt}'
    ) t1 
    on t0.uid= t1.uid and t0.server_date = date_add(t1.pt, 1)

    left outer join 
    (
        select
            uid, concat_ws('-', year, month, day) as pt, 
            nvl(sum(plan_amount)/100,0) as plan_amount,
            nvl(count(distinct concat_ws('-', loan_order_id, cast(duration_num as string))),0) as duration_num
        from 
            manhattan_dw.dwm_loan_repay_dur_sum_d_whole
        where
            concat_ws('-', year, month, day) between date_add('${start_pt}',-1) and '${end_pt}'
            and status in (1,3)
        group by uid, concat_ws('-', year, month, day)
    ) t2
    on t0.uid = t2.uid and t0.server_date = date_add(t2.pt, 1)

    left outer join 
    (
        select
            uid, concat_ws('-', year, month, day) as pt, 
            current_valid_coupon_num,
            max_use_value_amt,
            avg_use_value_amt,
            last_30_day_drip_taken_coupon_num
        from 
            riskmanage_dm.feature_individual_manhattan_drip_coupon
        where
            concat_ws('-', year, month, day) between date_add('${start_pt}',-1) and '${end_pt}'
    ) t3
    on t0.uid = t3.uid and t0.server_date = date_add(t3.pt, 1)

"
