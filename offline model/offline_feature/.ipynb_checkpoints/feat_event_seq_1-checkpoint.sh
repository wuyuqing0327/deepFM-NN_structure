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

drop table if exists manhattan_test.user_demand_model_event_seq1_${start_pt}_${end_pt};
create table manhattan_test.user_demand_model_event_seq1_${start_pt}_${end_pt} as 

select
    uid, funds_channel_id, tel, label, server_date,
    concat_ws('#', collect_list( 
            concat_ws(',', cast(datediff(server_date, node_server_time) as string) , cast(action_type as string) )
    )) as action_type_string
from 
    (
        select
            t0.*, t1.server_time as node_server_time, cast(action_type as string) as action_type, rk
        from 
            (
                select uid, funds_channel_id, tel, label, server_date
                from manhattan_test.dsd_user_loan_demand_samples_label_${start_pt}_${end_pt}
                where funds_channel_id is not null 
            ) t0 
            left outer join 
            (
                select 
                    tel, server_time, server_date, action_type, rk 
                from 
                (
                    select
                        tel, server_time, server_date, action_type,
                        row_number() over(partition by tel order by server_time desc) as rk 
                    from 
                        (
                            select
                                tel, 
                                eventid,
                                -- dsd_market_biopen,
                                -- dsd_market_setup_sw,
                                -- dsd_mycoupon_ck,
                                -- dsd_loan_list_sw,
                                -- dsd_loan_page_show,
                                -- dsd_loan_confirm_button_ck,
                                -- dsd_input_pay_password_popup,
                                -- dsd_face_rec,
                                -- dsd_loan_apply_submit_api_done,
                                server_time,
                                to_date(server_time) as server_date,
                                action_type
                            from 
                                (
                                    select 
                                        tel, 
                                        eventid, 
                                        from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)) as server_time,
                                        concat_ws('-', year, month, day) as pt,
                                        case 
                                            when 
                                            eventid = 'market_biopen' 
                                            and attrs['funds_credited_list'] like '30%'
                                            and attrs['page_name'] = 'market_home_page'
                                            then 1

                                            when 
                                            eventid = 'market_setup_sw' 
                                            and attrs['funds_credited_list'] like '30%'
                                            then 2

                                            when 
                                            eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk')
                                            and attrs['funds_credited_list'] like '30%'
                                            then 3

                                            when 
                                            eventid = 'market_loan_list_sw'
                                            and attrs['funds_credited_list'] like '30%'
                                            then 4

                                            when 
                                            eventid ='loan_page_show'
                                            and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                            then 5

                                            when 
                                            eventid = 'loan_confirm_button_ck'
                                            and  (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                            then 6

                                            when 
                                            eventid = 'input_pay_password_popup'
                                            and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                            then 7

                                            when    
                                            eventid in ('face_rec_button')
                                            and  (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                            then 8

                                            when 
                                            eventid = 'loan_apply_submit_api_done'
                                            and (attrs['company_id'] like '30%' or attrs['product_id'] like '2001')
                                            then 9

                                        else 0 end as action_type
                                    from
                                        manhattan_ods.ods_buddy_tracker
                                    where 
                                        concat_ws('-', year, month, day) between add_months('${start_date}', -6) and '${end_date}'
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
                                to_date(server_time) between add_months('${start_date}', -6) and '${end_date}'
                            group by 
                                tel, 
                                eventid,
                                server_time,
                                to_date(server_time),
                                action_type
                            order by 
                                tel, server_time asc
                        ) t0 
                ) t0 
                where 
                    action_type > 0 and tel is not null 
            ) t1 
            on t0.tel = t1.tel
        where
            t1.server_date >= add_months(t0.server_date, -6)
            and t1.server_date < t0.server_date
            and rk <= 30
        order by
            uid, server_date, node_server_time asc
    ) t0 
group by 
    uid, funds_channel_id, tel, label, server_date

"
