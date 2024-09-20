#!/bin/bash
source ~/.bash_profile

date=$1
echo "date is $date"
name=$2
echo "name is $name"

hive -e "
drop table if exists manhattan_test.dsd_user_loan_demand_repay_seq_node_offline;
create table manhattan_test.dsd_user_loan_demand_repay_seq_node_offline as 

with user_samples as (
   select 
   uid,server_date 
   from manhattan_test.dsd_user_loan_demand_samples_label_20200710_20200810 
   WHERE funds_channel_id is not null
   group by uid,server_date
)

select
    label.uid, label.server_date, t0.repay_node_string
from
(
    select * from user_samples
) label

left outer join
(
    SELECT
        uid, server_date, concat_ws(',', collect_list(repay_node_time)) as repay_node_string
    from
        (
            SELECT
                uid,
                server_date,
                repay_node_time,
                repay_node_rank
            from
                (
                    select
                        *, row_number() over(partition by uid, server_date order by repay_node_time desc) as repay_node_rank
                    from
                        (
                            SELECT
                                label.uid,
                                label.server_date,
                                repay_info.real_repay_time,
                                case
                                    when repay_info.real_repay_time is not null and repay_info.real_repay_time >= label.server_date then '0000-00-00'
                                else repay_info.real_repay_time end as repay_node_time
                            from
                                (
                                    select * from user_samples
                                ) label

                                left outer join
                                (
                                    select
                                        distinct
                                        uid,
                                        real_repay_time,
                                        to_date(real_repay_time) as real_repay_date
                                    from
                                        manhattan_dw.dwd_loan_repay_per_time_d_whole
                                    WHERE
                                        concat_ws('-',year,month,day)='2020-08-10'
                                        and real_repay_time like '20%'
                                        and repay_status = 1


                                ) repay_info
                                on label.uid = repay_info.uid 

                        ) t0
                    WHERE
                        repay_node_time <> '0000-00-00'
                ) t0
            WHERE
                repay_node_rank<=30
            order by
                uid, server_date, repay_node_rank asc
        ) t0
    group by
        uid, server_date
) t0
on label.uid = t0.uid 
and label.server_date = t0.server_date
"

