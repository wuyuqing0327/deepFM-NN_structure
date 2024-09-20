#!/bin/bash
source ~/.bash_profile

date=$1
echo "date is $date"
name=$2
echo "name is $name"

hive -e "
drop table manhattan_test.dsd_user_loan_demand_loan_seq_node_feature_new_offline;
create table manhattan_test.dsd_user_loan_demand_loan_seq_node_feature_new_offline as 

with loan_seq_node as (
    select
        uid, server_date, loan_node_time, to_date(loan_node_time) as loan_node_date
    from
        manhattan_test.dsd_user_loan_demand_loan_seq_node_offline t
        lateral view explode(split(loan_node_string,',')) b AS loan_node_time
)


select
    label.*
    , loan_succ --是否支用成功
    , principal/current_drip_loan_credit_line as principal_in_line_ratio--借款本金
    , total_duration --总期数
    , interest_rate --利息利率，扩大百万倍，按日计息
    , repay_method_1
    , repay_method_2
    , repay_method_3
    , is_have_coupon --是否有可用优惠券1:是,0:否
    , coupon_type_1
    , coupon_type_2
    , coupon_type_3
    , coupon_type_7
    , coupon_type_8
    , coupon_type_9
    , coupon_type_10
    , coupon_type_11
    , days_from_first_succ_loan  --距滴水贷首次支用成功时间
    , days_from_last_succ_loan    --距滴水贷最后一次支用成功时间
    , days_from_last_failed_loan    --距滴水贷最近支用失败时间
    , current_drip_loan_in_loan_disburse_num --当前在贷支用笔数
    , his_drip_loan_repay_done_num --滴水贷已还清笔数
    , his_drip_loan_repay_not_done_num --滴水贷尚未还清笔数总和
    , current_drip_loan_disburse_should_repayment_principal --当前支用应还款总本金
    , days_from_credit_succ_days --距离授信时间
    , current_drip_loan_credit_line_use_rate --滴水贷额度使用率
    , current_drip_loan_credit_line_available_rate --滴水贷剩余额度占比
    
from
    (
        select * from loan_seq_node
    ) label

    left outer join
    (
        select
            label.uid, 
            label.server_date, 
            label.loan_node_time,
            loan_info.loan_succ, --是否支用成功
            loan_info.principal, --借款本金
            loan_info.total_duration,--总期数
            loan_info.interest_rate,--利息利率，扩大百万倍，按日计息
            loan_info.repay_method_1,
            loan_info.repay_method_2,
            loan_info.repay_method_3,
            loan_info.is_have_coupon,--是否有可用优惠券1:是,0:否
            loan_info.coupon_type_1,
            loan_info.coupon_type_2,
            loan_info.coupon_type_3,
            loan_info.coupon_type_7,
            loan_info.coupon_type_8,
            loan_info.coupon_type_9,
            loan_info.coupon_type_10,
            loan_info.coupon_type_11
        from
            (
                select * from loan_seq_node
            ) label

            left outer join
            (
                select
                    uid,
                    create_time,
                    max(loan_succ) as loan_succ,
                    sum(principal) as principal,
                    sum(total_duration) as total_duration,
                    sum(interest_rate) as interest_rate,
                    max(repay_method_1) as repay_method_1,
                    max(repay_method_2) as repay_method_2,
                    max(repay_method_3) as repay_method_3,
                    max(is_have_coupon) as is_have_coupon,
                    max(coupon_type_1) as coupon_type_1,
                    max(coupon_type_2) as coupon_type_2,
                    max(coupon_type_3) as coupon_type_3,
                    max(coupon_type_7) as coupon_type_7,
                    max(coupon_type_8) as coupon_type_8,
                    max(coupon_type_9) as coupon_type_9,
                    max(coupon_type_10) as coupon_type_10,
                    max(coupon_type_11) as coupon_type_11
                from 
                (
                    select
                        uid, 
                        create_time,
                        case when status in (2,4) then 1 else 0 end as loan_succ, --是否支用成功
                        principal/100 as principal,  --借款本金
                        total_duration, --  总期数
                        interest_rate, --利息利率，扩大百万倍，按日计息
                        case when repay_method = 1 then 1 else 0 end as repay_method_1,
                        case when repay_method = 2 then 1 else 0 end as repay_method_2,
                        case when repay_method = 3 then 1 else 0 end as repay_method_3,
                        is_have_coupon, --  是否有可用优惠券1:是,0:否
                        case when coupon_type = 1 then 1 else 0 end as coupon_type_1,
                        case when coupon_type = 2 then 1 else 0 end as coupon_type_2,
                        case when coupon_type = 3 then 1 else 0 end as coupon_type_3,
                        case when coupon_type = 7 then 1 else 0 end as coupon_type_7,
                        case when coupon_type = 8 then 1 else 0 end as coupon_type_8,
                        case when coupon_type = 9 then 1 else 0 end as coupon_type_9,
                        case when coupon_type = 10 then 1 else 0 end as coupon_type_10,
                        case when coupon_type = 11 then 1 else 0 end as coupon_type_11
                        
                    from
                        manhattan_dw.dwd_loan_loan_detail_new_d_whole
                    where
                        concat_ws('-', year, month, day) = '2020-08-10'
                        and product_id = '2001'
                ) t 
                group by t.uid,t.create_time
            ) loan_info
            on label.uid = loan_info.uid 
            and label.loan_node_time = loan_info.create_time
    ) this_loan
    on label.uid = this_loan.uid 
    and label.server_date = this_loan.server_date 
    and label.loan_node_time = this_loan.loan_node_time


    left outer join
    (
        SELECT
            label.uid, label.server_date, label.loan_node_time
            , datediff(label.loan_node_date, first_drip_loan_disburse_succ_time)    as days_from_first_succ_loan  --滴水贷首次支用成功时间
            , datediff(label.loan_node_date, last_drip_loan_disburse_succ_time) as days_from_last_succ_loan    --滴水贷最后一次支用成功时间,未成功(1971-01-01 00:00:00)
            , datediff(label.loan_node_date, last_drip_loan_disburse_failed_time) as days_from_last_failed_loan    --滴水贷最近支用失败时间,未失败(1971-01-01 00:00:00)
            , current_drip_loan_in_loan_disburse_num --当前在贷支用笔数
            , his_drip_loan_repay_done_num --滴水贷已还清笔数
            , his_drip_loan_repay_not_done_num --滴水贷尚未还清笔数总和
            , current_drip_loan_disburse_should_repayment_principal --当前支用应还款总本金
        from
            (
                select * from loan_seq_node
            ) label

            left outer join
            (
                SELECT
                    uid
                    , first_drip_loan_disburse_succ_time --滴水贷首次支用成功时间
                    , last_drip_loan_disburse_succ_time --滴水贷最后一次支用成功时间
                    , last_drip_loan_disburse_failed_time --    滴水贷最近支用失败时间
                    , current_drip_loan_in_loan_disburse_num --当前在贷支用笔数
                    , his_drip_loan_repay_done_num --滴水贷已还清笔数
                    , his_drip_loan_repay_not_done_num --滴水贷尚未还清笔数总和
                    , current_drip_loan_disburse_should_repayment_principal --当前支用应还款总本金
                    , CONCAT_WS('-', year, month, day) as stat_date
                from
                    riskmanage_dm.feature_individual_manhattan_drip_loan_disburse

                WHERE
                    CONCAT_WS('-', year, month, day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
            ) loan_feature
            on label.uid = loan_feature.uid and label.loan_node_date = date_add(loan_feature.stat_date,1)

    ) loan_history
    on label.uid = loan_history.uid 
    and label.server_date = loan_history.server_date 
    and label.loan_node_time = loan_history.loan_node_time

    left outer join
    (
        select
            t1.*,
            datediff(t1.loan_node_date, t1.credit_last_success_time) as days_from_credit_succ_days, --距离授信时间
            t2.current_drip_loan_credit_line_used , --滴水贷已用额度
            t2.current_drip_loan_credit_line_used/t2.current_drip_loan_credit_line as current_drip_loan_credit_line_use_rate, --滴水贷额度使用率
            t2.current_drip_loan_credit_line_available , --滴水贷剩余额度
            t2.current_drip_loan_credit_line_available / t2.current_drip_loan_credit_line as current_drip_loan_credit_line_available_rate, --滴水贷剩余额度占比
            t2.current_drip_loan_credit_line --滴水贷当前额度
        from 
        (
            select 
                t.uid,
                server_date,
                loan_node_time,
                loan_node_date,
                funds_channel_id,
                cfrnid,
                success_time,
                credit_last_success_time           
            from
            (
                select 
                    label.uid,
                    server_date,
                    loan_node_time,
                    loan_node_date,
                    funds_channel_id,
                    cfrnid,
                    success_time,
                    credit_last_success_time,
                    row_number() over(partition by label.uid, server_date, loan_node_time order by success_time desc) as rn_1
                from
                (
                    select * from loan_seq_node
                ) label
              
                left outer join
                (
                    select
                        uid,
                        funds_channel_id,
                        cfrnid,
                        success_time,
                        to_date(success_time) as credit_last_success_time,
                        CONCAT_WS('-', year, month, day) as stat_date
                    from manhattan_dw.dwd_loan_user_credit_d_whole
                    where concat_ws('-',year,month,day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
                        and success_time like '20%'
                        and product_id=2001
                        and status = 1
                    group by uid,funds_channel_id,cfrnid,success_time,CONCAT_WS('-', year, month, day)
                ) t0
                on label.uid = t0.uid and label.loan_node_date = date_add(t0.stat_date,1)
                where label.loan_node_time > t0.success_time
            ) t 
            where rn_1 = 1
        ) t1

        left outer join
        (
            select
                cfrnid,
                funds_channel_id,
                fixed_limit_used /100 as current_drip_loan_credit_line_used ,
                fixed_limit_available /100 as current_drip_loan_credit_line_available,
                fixed_limit/100 as current_drip_loan_credit_line,
                CONCAT_WS('-', year, month, day) as stat_date
            from manhattan_dw.dwd_loan_credit_limit_d_whole
            where concat_ws('-',year,month,day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
                and product_id=2001
                and fixed_limit_status =0
                and (di_status=0  or di_status is null)
            group by cfrnid,funds_channel_id,fixed_limit_used,fixed_limit_available,fixed_limit,CONCAT_WS('-', year, month, day)
        ) t2
        on t1.cfrnid=t2.cfrnid and t1.funds_channel_id = t2.funds_channel_id and t1.loan_node_date = date_add(t2.stat_date ,1)

    ) loan_line 
    on label.uid = loan_line.uid 
    and label.server_date = loan_line.server_date 
    and label.loan_node_time = loan_line.loan_node_time

"
