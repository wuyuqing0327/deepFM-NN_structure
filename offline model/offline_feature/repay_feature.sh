#!/bin/bash
source ~/.bash_profile

date=$1
echo "date is $date"
name=$2
echo "name is $name"

hive -e "
create table manhattan_test.dsd_user_loan_demand_repay_seq_node_feature_offline as

with repay_seq_node as (
    select
        uid, server_date, repay_node_time, to_date(repay_node_time) as repay_node_date
    from
        manhattan_test.dsd_user_loan_demand_repay_seq_node_offline t
        lateral view explode(split(repay_node_string,',')) b AS repay_node_time
)

SELECT
    label.uid, 
    label.server_date, 
    label.repay_node_time
    
    , repay_ratio --还款金额占额度比利
    , repay_mode_1  --还款模式（1:代扣 2:主动）
    , repay_mode_2
    , repay_channel_1
    , repay_channel_2
    , repay_channel_3
    , repay_channel_5
    , repay_channel_7
    , repay_channel_10
    , repay_channel_13
    , repay_channel_99
    , repay_channel_100
    , repay_interest  --bigint  本次还款利息
    , repay_penalty   --bigint  本次还款罚息
    , from_last_loan_issue_days --距上一次支用时间
    , from_last_repay_succ_days --距上一次还款时间
    , from_plan_end_days --距最近一个账单日时间
from
    (
       select * from repay_seq_node
    ) label

    left outer join
    (
        select
            label.uid, label.server_date, label.repay_node_time
            
            , repay_amount/current_drip_loan_credit_line as repay_ratio
            , repay_mode_1  --bigint  还款模式（1:代扣 2:主动）
            , repay_mode_2
            , repay_channel_1
            , repay_channel_2
            , repay_channel_3
            , repay_channel_5
            , repay_channel_7
            , repay_channel_10
            , repay_channel_13
            , repay_channel_99
            , repay_channel_100
            , repay_interest  --bigint  本次还款利息
            , repay_penalty   --bigint  本次还款罚息
            , datediff(label.repay_node_date,last_loan_issue_time) as from_last_loan_issue_days
            , datediff(label.repay_node_date,last_repay_succ_time) as from_last_repay_succ_days

        from
        (
            select * from repay_seq_node
        ) label

        left outer join
        (
            select
                uid
                , real_repay_time
                , max(repay_mode_1) as repay_mode_1
                , max(repay_mode_2) as repay_mode_2
                , max(repay_channel_1) as repay_channel_1
                , max(repay_channel_2) as repay_channel_2
                , max(repay_channel_3) as repay_channel_3
                , max(repay_channel_5) as repay_channel_5
                , max(repay_channel_7) as repay_channel_7
                , max(repay_channel_10) as repay_channel_10
                , max(repay_channel_13) as repay_channel_13
                , max(repay_channel_99) as repay_channel_99
                , max(repay_channel_100) as repay_channel_100
                , sum(repay_amount) as repay_amount
                , sum(repay_interest) as repay_interest
                , sum(repay_penalty) as repay_penalty
            from
            (
                select
                    uid
                    , real_repay_time  --实际还款时间
                    , case when repay_mode = 1 then 1 else 0 end as repay_mode_1
                    , case when repay_mode = 2 then 1 else 0 end as repay_mode_2
                    , case when repay_channel = 1 then 1 else 0 end as repay_channel_1
                    , case when repay_channel = 2 then 1 else 0 end as repay_channel_2
                    , case when repay_channel = 3 then 1 else 0 end as repay_channel_3
                    , case when repay_channel = 5 then 1 else 0 end as repay_channel_5
                    , case when repay_channel = 7 then 1 else 0 end as repay_channel_7
                    , case when repay_channel = 10 then 1 else 0 end as repay_channel_10
                    , case when repay_channel = 13 then 1 else 0 end as repay_channel_13
                    , case when repay_channel = 99 then 1 else 0 end as repay_channel_99
                    , case when repay_channel = 100 then 1 else 0 end as repay_channel_100
                    , repay_amount/100 as repay_amount  --bigint  本次还款金额(本+利+罚)（单位：分）
                    , repay_interest/100 as repay_interest  --bigint  本次还款利息（单位：分）
                    , repay_penalty/100 as repay_penalty   --bigint  本次还款罚息（单位：分）
                from
                    manhattan_dw.dwd_loan_repay_per_time_d_whole
                where
                    concat_ws('-', year, month, day) = '2020-08-10'
                    and product_id = '2001'
            ) t
            group by t.uid,t.real_repay_time

        ) repay_info
        on label.uid = repay_info.uid
        and label.repay_node_time = repay_info.real_repay_time

        left outer join
        (
            select
                t1.*,
                t2.current_drip_loan_credit_line --滴水贷当前额度
            from 
            (
                select 
                    t.uid,
                    server_date,
                    repay_node_time,
                    repay_node_date,
                    funds_channel_id,
                    cfrnid,
                    success_time,
                    credit_last_success_time           
                from
                (
                    select 
                        label.uid,
                        server_date,
                        repay_node_time,
                        repay_node_date,
                        funds_channel_id,
                        cfrnid,
                        success_time,
                        credit_last_success_time,
                        row_number() over(partition by label.uid, server_date, repay_node_time order by success_time desc) as rn_1
                    from
                    (
                        select * from repay_seq_node
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
                    ) t0
                    on label.uid = t0.uid and label.repay_node_date = date_add(t0.stat_date,1)
                    where label.repay_node_time > t0.success_time
                ) t 
                where rn_1 = 1
            ) t1

            left outer join
            (
                select
                    cfrnid,
                    funds_channel_id,
                    fixed_limit/100 as current_drip_loan_credit_line,
                    CONCAT_WS('-', year, month, day) as stat_date
                from manhattan_dw.dwd_loan_credit_limit_d_whole
                where concat_ws('-',year,month,day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
                    and product_id=2001
                    and fixed_limit_status =0
                    and (di_status=0 or di_status is null)
            ) t2
            on t1.cfrnid=t2.cfrnid and t1.funds_channel_id = t2.funds_channel_id and t1.repay_node_date = date_add(t2.stat_date ,1)
        ) credit
        on label.uid = credit.uid
        and label.server_date = credit.server_date
        and label.repay_node_time = credit.repay_node_time

        left outer join
        (   
            select
                uid,
                last_loan_issue_time,
                last_repay_succ_time,
                concat_ws('-',year, month, day) as stat_date
            from manhattan_dw.dwm_loan_repay_user_sum_d_whole
            where concat_ws('-', year, month, day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
            and product_id = 2001
        ) repay_time 
        on label.uid = repay_time.uid
        and label.repay_node_date = repay_time.stat_date

    ) this_repay
    on label.uid = this_repay.uid
    and label.server_date = this_repay.server_date
    and label.repay_node_time = this_repay.repay_node_time

    left outer join
    (
        select 
            uid,
            server_date,
            repay_node_time,
            repay_node_date,
            datediff(min(plan_end_time),repay_node_date) as from_plan_end_days
        from
        (
            select
                label.*,
                stat_date,
                plan_end_time
            from 
            (
                select * from repay_seq_node
            ) label

            left join
            (
                select 
                    uid,
                    concat_ws('-',year, month, day) as stat_date,
                    to_date(plan_end_time) plan_end_time
                from manhattan_dw.dwm_loan_repay_dur_sum_d_whole
                where concat_ws('-', year, month, day) BETWEEN date_add(add_months('2020-07-10', -6),-1) and '2020-08-10'
                    and status = 1
                    and to_date(plan_end_time) >= concat_ws('-',year, month, day)

            ) dwm_loan_repay_dur_sum_d_whole 
            on label.uid = dwm_loan_repay_dur_sum_d_whole.uid 
            and label.repay_node_date = date_add(dwm_loan_repay_dur_sum_d_whole.stat_date,1)
        ) repay_dur
        group by uid,server_date,repay_node_time,repay_node_date

    ) repay_plan
    on label.uid = repay_plan.uid
    and label.server_date = repay_plan.server_date
    and label.repay_node_time = repay_plan.repay_node_time

"

