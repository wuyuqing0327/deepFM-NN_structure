#!/bin/bash
source ~/.bash_profile

date=$1
name=$2


hive -e "
create table manhattan_test.dsd_user_loan_demand_loan_repay_event_seq_strings_offline_0909 as

select
    users.*,
    
    loan.seq_feature_string as loan_seq_feature_string,
    repay.seq_feature_string as repay_seq_feature_string,
    event2.loan_chain_string as event2_chain_seq_feature_string
from
(
select *
from manhattan_test.dsd_user_loan_demand_samples_label_20200710_20200810 
) users

left join
(
select *
from manhattan_test.dsd_user_loan_demand_loan_seq_all_feature_offline
) loan
on users.uid = loan.uid and users.server_date = loan.server_date

left join
(
select *
from manhattan_test.dsd_user_loan_demand_repay_seq_all_feature_offline
) repay
on users.uid = repay.uid and users.server_date = repay.server_date

left join
(
select *
from manhattan_test.user_demand_model_event_seq2_20200710_20200810_spark_2
) event2
on users.uid = event2.uid and users.server_date = event2.server_date

"

