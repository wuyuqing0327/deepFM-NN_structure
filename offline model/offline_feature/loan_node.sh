#!/bin/bash
source ~/.bash_profile


hive -e "
drop table if exists manhattan_test.dsd_user_loan_demand_loan_seq_node_offline;
create table manhattan_test.dsd_user_loan_demand_loan_seq_node_offline as

with user_samples as (
   select uid,server_date 
   from manhattan_test.dsd_user_loan_demand_samples_label_20200710_20200810 
   WHERE funds_channel_id is not null
   group by uid,server_date
)

select
    label.uid, label.server_date, t0.loan_node_string
from
(
    select * from user_samples
) label

left outer join
(
    SELECT
        uid, server_date, concat_ws(',', collect_list(loan_node_time)) as loan_node_string
    from
        (
            SELECT
                uid,
                server_date,
                loan_node_time,
                loan_node_rank
            from
                (
                    select
                        *, row_number() over(partition by uid, server_date order by loan_node_time desc) as loan_node_rank
                    from
                        (
                            SELECT
                                label.uid,
                                label.server_date,
                                case
                                    when loan_info.create_time is not null and loan_info.create_time >= label.server_date then '0000-00-00'
                                else loan_info.create_time end as loan_node_time
                            from
                                (
                                    select * from user_samples
                                ) label

                                left outer join
                                (
                                    select
                                        uid,
                                        create_time

                                    from
                                        manhattan_dw.dwd_loan_loan_detail_new_d_whole
                                    WHERE
                                        concat_ws('-',year,month,day)='2020-08-10'
                                        and status in (1,2,3,4)
                                        and create_time like '20%'
                                    group by uid,create_time
                                ) loan_info
                                on label.uid = loan_info.uid 
                        ) t0
                    WHERE
                        loan_node_time <> '0000-00-00'
                ) t0
            WHERE
                loan_node_rank<=10
            order by
                uid, server_date, loan_node_rank asc
        ) t0
    group by
        uid, server_date
) t0
on label.uid = t0.uid and label.server_date = t0.server_date
"



