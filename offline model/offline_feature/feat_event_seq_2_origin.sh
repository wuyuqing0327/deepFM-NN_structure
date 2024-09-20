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
drop table if exsist manhattan_test.user_demand_model_event_seq2_${start_pt}_${end_pt}_origin;
create table manhattan_test.user_demand_model_event_seq2_${start_pt}_${end_pt}_origin as 

select
    uid, tel, server_date, label, 
    concat_ws('#', collect_list(loan_chain)) as loan_chain_string
from 
    (
        select
            uid, tel, funds_channel_id, label, server_date, node_server_date, rk, 
            concat_ws(',', 
                cast(datediff(server_date, node_server_date) as string), 

                loan_chain,
                cast(credit_line_level_1 as string),
                cast(credit_line_level_2 as string),
                cast(credit_line_level_3 as string),
                cast(credit_line_level_4 as string),
                cast(credit_line_level_5 as string),
                cast(credit_line_level_6 as string),

                cast(current_drip_interest_rate_level_1 as string),
                cast(current_drip_interest_rate_level_2 as string),
                cast(current_drip_interest_rate_level_3 as string),
                cast(current_drip_interest_rate_level_4 as string),
                cast(current_drip_interest_rate_level_5 as string),
                cast(current_drip_interest_rate_level_6 as string),
                cast(current_drip_interest_rate_level_7 as string),
                cast(current_drip_interest_rate_level_8 as string),

                cast(current_drip_credit_line_balance_level_1 as string),
                cast(current_drip_credit_line_balance_level_2 as string),
                cast(current_drip_credit_line_balance_level_3 as string),
                cast(current_drip_credit_line_balance_level_4 as string),
                cast(current_drip_credit_line_balance_level_5 as string),
                cast(current_drip_credit_line_balance_level_6 as string),

                cast(last_credit_success_start_time_level_1 as string),
                cast(last_credit_success_start_time_level_2 as string),
                cast(last_credit_success_start_time_level_3 as string),
                cast(last_credit_success_start_time_level_4 as string),
                cast(last_credit_success_start_time_level_5 as string),
                cast(last_credit_success_start_time_level_6 as string),

                cast(plan_amount_level_1 as string),
                cast(plan_amount_level_2 as string),
                cast(plan_amount_level_3 as string),
                cast(plan_amount_level_4 as string),
                cast(plan_amount_level_5 as string),
                cast(plan_amount_level_6 as string),
                cast(plan_amount_level_7 as string),
                cast(plan_amount_level_8 as string),
                cast(plan_amount_level_9 as string),

                cast(duration_num_level_1 as string),
                cast(duration_num_level_2 as string),
                cast(duration_num_level_3 as string),
                cast(duration_num_level_4 as string),
                cast(duration_num_level_5 as string),
                cast(duration_num_level_6 as string),

                cast(current_valid_coupon_num_level_1 as string),
                cast(current_valid_coupon_num_level_2 as string),
                cast(current_valid_coupon_num_level_3 as string),
                cast(current_valid_coupon_num_level_4 as string)
            ) as loan_chain
        from 
            (
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
                    case when round(months_between(t0.server_date, to_date(last_credit_success_start_time)),0) >= 25 then 1 else 0 end as last_credit_success_start_time_level_6,

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
                        select
                            t0.*,
                            t1.server_date as node_server_date,
                            t1.loan_chain,
                            row_number() over(partition by t0.uid, t0.server_date order by t1.server_date desc) as rk
                        from
                            (
                                select uid, funds_channel_id, tel, label, server_date
                                from manhattan_test.dsd_user_loan_demand_samples_label_20200701_20200725
                                where funds_channel_id is not null
                            ) t0

                            left outer join
                            (
                                select
                                    tel, server_date,
                                    concat_ws(',',
                                        cast(dsd_market_biopen_tag as string),
                                        cast(dsd_market_setup_sw_tag as string),
                                        cast(dsd_mycoupon_ck_tag as string),
                                        cast(dsd_loan_list_sw_tag as string),
                                        cast(dsd_loan_page_show_tag as string),
                                        cast(dsd_loan_confirm_button_ck_tag  as string),
                                        cast(dsd_input_pay_password_popup_tag as string),
                                        cast(dsd_face_rec_tag as string),
                                        cast(dsd_loan_apply_submit_api_done_tag as string)
                                    ) as loan_chain
                                from
                                    (
                                        select
                                            tel,server_date,
                                            case when dsd_market_biopen>0 then 1 else 0 end as dsd_market_biopen_tag,
                                            case when dsd_market_biopen * dsd_market_setup_sw >0 then 1 else 0 end as dsd_market_setup_sw_tag,
                                            case when dsd_market_biopen * dsd_market_setup_sw * dsd_mycoupon_ck>0 then 1 else 0 end as dsd_mycoupon_ck_tag,
                                            case when dsd_market_biopen * dsd_market_setup_sw * dsd_loan_list_sw>0 then 1 else 0 end as dsd_loan_list_sw_tag,
                                            case when dsd_market_biopen * dsd_loan_page_show>0 then 1 else 0 end as dsd_loan_page_show_tag,
                                            case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck>0 then 1 else 0 end as dsd_loan_confirm_button_ck_tag,
                                            case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_input_pay_password_popup >0 then 1 else 0 end as dsd_input_pay_password_popup_tag,
                                            case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_face_rec >0 then 1 else 0 end as dsd_face_rec_tag,
                                            case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_loan_apply_submit_api_done >0 then 1 else 0 end as dsd_loan_apply_submit_api_done_tag
                                        from
                                        (
                                            select
                                                tel, server_date,
                                                count(distinct case when dsd_market_biopen=1 then tel else null end) as dsd_market_biopen,
                                                count(distinct case when dsd_market_setup_sw=1 then tel else null end) as dsd_market_setup_sw,
                                                count(distinct case when dsd_mycoupon_ck=1 then tel else null end) as dsd_mycoupon_ck,
                                                count(distinct case when dsd_loan_list_sw=1 then tel else null end) as dsd_loan_list_sw,
                                                count(distinct case when dsd_loan_page_show=1 then tel else null end) as dsd_loan_page_show,
                                                count(distinct case when dsd_loan_confirm_button_ck=1 then tel else null end) as dsd_loan_confirm_button_ck,
                                                count(distinct case when dsd_input_pay_password_popup=1 then tel else null end) as dsd_input_pay_password_popup,
                                                count(distinct case when dsd_face_rec=1 then tel else null end) as dsd_face_rec,
                                                count(distinct case when dsd_loan_apply_submit_api_done=1 then tel else null end) as dsd_loan_apply_submit_api_done
                                            from
                                                (
                                                    select
                                                        tel,
                                                        eventid,
                                                        dsd_market_biopen,
                                                        dsd_market_setup_sw,
                                                        dsd_mycoupon_ck,
                                                        dsd_loan_list_sw,
                                                        dsd_loan_page_show,
                                                        dsd_loan_confirm_button_ck,
                                                        dsd_input_pay_password_popup,
                                                        dsd_face_rec,
                                                        dsd_loan_apply_submit_api_done,
                                                        server_time,
                                                        to_date(server_time) as server_date
                                                    from
                                                        (
                                                            select
                                                                tel,
                                                                eventid,
                                                                from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)) as server_time,
                                                                concat_ws('-', year, month, day) as pt,
                                                                case when
                                                                    eventid = 'market_biopen'
                                                                    and attrs['funds_credited_list'] like '30%'
                                                                    and attrs['page_name'] = 'market_home_page'
                                                                    then 1 else 0
                                                                end as dsd_market_biopen,
                
                                                                case when
                                                                    eventid = 'market_setup_sw'
                                                                    and attrs['funds_credited_list'] like '30%'
                                                                    then 1 else 0
                                                                end as dsd_market_setup_sw,
                
                                                                case when
                                                                    eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk')
                                                                    and attrs['funds_credited_list'] like '30%'
                                                                    then 1 else 0
                                                                end as dsd_mycoupon_ck,
                
                                                                case when
                                                                    eventid = 'market_loan_list_sw'
                                                                    and attrs['funds_credited_list'] like '30%'
                                                                    then 1 else 0
                                                                end as dsd_loan_list_sw,
                
                                                                case when
                                                                    eventid ='loan_page_show'
                                                                    and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                                                    then 1 else 0
                                                                end as dsd_loan_page_show,
                
                                                                case when
                                                                    eventid = 'loan_confirm_button_ck'
                                                                    and  (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                                                    then 1 else 0
                                                                end as dsd_loan_confirm_button_ck,
                
                                                                case when
                                                                    eventid = 'input_pay_password_popup'
                                                                    and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                                                    then 1 else 0
                                                                end as dsd_input_pay_password_popup,
                
                                                                case when
                                                                    eventid in ('face_rec_button')
                                                                    and  (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                                                    then 1 else 0
                                                                end as dsd_face_rec,
                
                                                                case when
                                                                    eventid = 'loan_apply_submit_api_done'
                                                                    and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                                                    then 1 else 0
                                                                end as dsd_loan_apply_submit_api_done
                
                                                            from
                                                                manhattan_ods.ods_buddy_tracker
                                                            where
                                                                concat_ws('-', year, month, day) between add_months('${start_pt}', -6) and '${end_pt}'
                                                                and eventid in (
                                                                    'market_biopen',
                                                                    'market_setup_sw',
                                                                    'market_loan_list_sw',
                                                                    'loan_page_show',
                                                                    'loan_confirm_button_ck',
                                                                    'input_pay_password_popup',
                                                                    -- 'fe_input_pay_password_set_pwd_show',
                                                                    -- 'fe_input_pay_password_close',
                                                                    'face_rec_button',
                                                                    -- 'call_face_recognize_result',
                                                                    -- 'face_id_enter_button_result',
                                                                    -- 'face_check_face_result_success',
                                                                    -- 'fe_face_recognize_res',
                                                                    'loan_apply_submit_api_done',
                                                                    'market_setup_mycoupon_ck',
                                                                    'setup_mycoupon_sk')
                                                        ) t0
                                                    where
                                                        to_date(server_time) between add_months('${start_pt}', -6) and '${end_pt}'
                                                    group by
                                                        tel,
                                                        eventid,
                                                        dsd_market_biopen,
                                                        dsd_market_setup_sw,
                                                        dsd_mycoupon_ck,
                                                        dsd_loan_list_sw,
                                                        dsd_loan_page_show,
                                                        dsd_loan_confirm_button_ck,
                                                        dsd_input_pay_password_popup,
                                                        dsd_face_rec,
                                                        dsd_loan_apply_submit_api_done,
                                                        server_time,
                                                        to_date(server_time)
                                                ) t0
                                            group by
                                                tel, server_date
                                        ) t0
                                    ) t0
                                order by
                                    tel, server_date desc
                            ) t1
                            on t0.tel = t1.tel
                        where
                            t1.server_date >= add_months(t0.server_date, -6)
                            and t1.server_date < t0.server_date
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
                            concat_ws('-', year, month, day) between date_add(add_months('${start_pt}', -6),-1) and '${end_pt}'
                    ) t1 
                    on t0.uid= t1.uid and t0.node_server_date = date_add(t1.pt, 1)

                    left outer join 
                    (
                        select
                            uid, concat_ws('-', year, month, day) as pt, 
                            nvl(sum(plan_amount)/100,0) as plan_amount,
                            nvl(count(distinct concat_ws('-', loan_order_id, cast(duration_num as string))),0) as duration_num
                        from 
                            manhattan_dw.dwm_loan_repay_dur_sum_d_whole
                        where
                            concat_ws('-', year, month, day) between date_add(add_months('${start_pt}', -6),-1) and '${end_pt}'
                            and status in (1,3)
                        group by uid, concat_ws('-', year, month, day)
                    ) t2
                    on t0.uid = t2.uid and t0.node_server_date = date_add(t2.pt, 1)

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
                            concat_ws('-', year, month, day) between date_add(add_months('${start_pt}', -6),-1) and '${end_pt}'
                    ) t3
                    on t0.uid = t3.uid and t0.node_server_date = date_add(t3.pt, 1)
            ) t0
            order by 
            	uid, server_date, node_server_date asc
    ) t0 
where
    rk <= 20
group by 
    uid, tel, server_date, label 


"

