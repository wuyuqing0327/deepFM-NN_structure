#!/bin/bash
source ~/.bash_profile

date=$1
echo "date is $date"
name=$2
echo "name is $name"

hive -e "
drop table manhattan_test.dsd_user_loan_demand_loan_seq_all_feature_offline;
create table manhattan_test.dsd_user_loan_demand_loan_seq_all_feature_offline as 

select
    label.*,
    t0.seq_feature_string
from
(

   select uid,server_date,label  
   from manhattan_test.dsd_user_loan_demand_samples_label_20200710_20200810 
   WHERE funds_channel_id is not null
   group by uid,server_date,label

) label

left outer join
(
    SELECT
        uid,
        server_date,
        collect_list(node_feature) as seq_feature_string
    from
        (
            SELECT
                uid,
                server_date,
                loan_node_time,
                concat_ws(','
                , loan_node_time
                , cast( round( nvl( loan_succ, 0) , 4) as string)
                , cast( round( nvl( principal_in_line_ratio, 0) , 4) as string)
                , cast( round( nvl( total_duration, 0) , 4) as string)
                , cast( round( nvl( interest_rate, 0) , 4) as string)
                , cast( round( nvl( repay_method_1, 0) , 4) as string)
                , cast( round( nvl( repay_method_2, 0) , 4) as string)
                , cast( round( nvl( repay_method_3, 0) , 4) as string)
                , cast( round( nvl( is_have_coupon, 0) , 4) as string)
                , cast( round( nvl( coupon_type_1, 0) , 4) as string)
                , cast( round( nvl( coupon_type_2, 0) , 4) as string)
                , cast( round( nvl( coupon_type_3, 0) , 4) as string)
                , cast( round( nvl( coupon_type_7, 0) , 4) as string)
                , cast( round( nvl( coupon_type_8, 0) , 4) as string)
                , cast( round( nvl( coupon_type_9, 0) , 4) as string)
                , cast( round( nvl( coupon_type_10, 0) , 4) as string)
                , cast( round( nvl( coupon_type_11, 0) , 4) as string)
                , cast( round( nvl( days_from_first_succ_loan, 0) , 4) as string)
                , cast( round( nvl( days_from_last_succ_loan, 0) , 4) as string)
                , cast( round( nvl( days_from_last_failed_loan, 0) , 4) as string)
                , cast( round( nvl( current_drip_loan_in_loan_disburse_num, 0) , 4) as string)
                , cast( round( nvl( his_drip_loan_repay_done_num, 0) , 4) as string)
                , cast( round( nvl( his_drip_loan_repay_not_done_num, 0) , 4) as string)
                , cast( round( nvl( current_drip_loan_disburse_should_repayment_principal, 0) , 4) as string)
                , cast( round( nvl( days_from_credit_succ_days, 0) , 4) as string)
                , cast( round( nvl( current_drip_loan_credit_line_use_rate, 0) , 4) as string)
                , cast( round( nvl( current_drip_loan_credit_line_available_rate, 0) , 4) as string)
                ) as node_feature
            from
                manhattan_test.dsd_user_loan_demand_loan_seq_node_feature_new_offline
            order by
                uid, server_date, loan_node_time asc
        ) t0
    group by
        uid,
        server_date
) t0
on label.uid = t0.uid 
and label.server_date = t0.server_date
"

