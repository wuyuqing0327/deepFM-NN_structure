#!/bin/bash
source ~/.bash_profile

date=$1
name=$2


hive -e "
select * from manhattan_test.dsd_user_loan_demand_loan_repay_event_seq_strings_offline
" > ../data/dsd_user_loan_demand_loan_repay_event_seq_strings_offline.csv
