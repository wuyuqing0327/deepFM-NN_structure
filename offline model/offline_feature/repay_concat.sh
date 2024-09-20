#!/bin/bash
source ~/.bash_profile

date=$1
echo "date is $date"
name=$2
echo "name is $name"
hive -e "
drop table if exists manhattan_test.dsd_user_loan_demand_repay_seq_all_feature_offline;
create table manhattan_test.dsd_user_loan_demand_repay_seq_all_feature_offline as 


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
                    repay_node_time,
                    concat_ws(','
                    , repay_node_time
                    , cast( round( nvl(repay_ratio, 0), 4) as string)
                    , cast( round( nvl(repay_mode_1, 0), 4) as string)
                    , cast( round( nvl(repay_mode_2, 0), 4) as string)
                    , cast( round( nvl(repay_channel_1, 0), 4) as string)
                    , cast( round( nvl(repay_channel_2, 0), 4) as string)
                    , cast( round( nvl(repay_channel_3, 0), 4) as string)
                    , cast( round( nvl(repay_channel_5, 0), 4) as string)
                    , cast( round( nvl(repay_channel_7, 0), 4) as string)
                    , cast( round( nvl(repay_channel_10, 0), 4) as string)
                    , cast( round( nvl(repay_channel_13, 0), 4) as string)
                    , cast( round( nvl(repay_channel_99, 0), 4) as string)
                    , cast( round( nvl(repay_channel_100, 0), 4) as string)
                    , cast( round( nvl(repay_interest, 0), 4) as string)
                    , cast( round( nvl(repay_penalty, 0), 4) as string)
                    , cast( round( nvl(from_last_loan_issue_days, 0), 4) as string)
                    , cast( round( nvl(from_last_repay_succ_days, 0), 4) as string)
                    , cast( round( nvl(from_plan_end_days, 0), 4) as string)


                    ) as node_feature
                from
                    manhattan_test.dsd_user_loan_demand_repay_seq_node_feature_offline
                
                order by
                    uid, server_date, repay_node_time asc
            ) t0
        group by
            uid,
            server_date
    ) t0
    on label.uid = t0.uid
    and label.server_date = t0.server_date
"


