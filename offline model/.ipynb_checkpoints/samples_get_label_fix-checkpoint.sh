#!/bin/.bash
source ~/.bash_profile

start_date=$1
end_date=$2

echo "start date for samples: '${start_date}'"
echo "end date for samples: '${end_date}'"

start=`date -d"-0 day $start_date" '+%Y%m%d'`
end=`date -d"-0 day $end_date" '+%Y%m%d'`

echo "${start}"
echo "${end}"

hive -e "
drop table if exists manhattan_test.dsd_user_loan_demand_samples_${start}_${end}; 
create table manhattan_test.dsd_user_loan_demand_samples_${start}_${end} as 

select
    t0.*, t1.funds_channel_id
from 
(
    select
        t0.*, 
        t1.uid
    from 
        (
            select 
                tel,server_date,
                case when dsd_market_biopen=1 then 1 else 0 end as dsd_market_biopen_tag,
                case when dsd_market_biopen * dsd_market_setup_sw = 1 then 1 else 0 end as dsd_market_setup_sw_tag,
                case when dsd_market_biopen * dsd_loan_page_show = 1 then 1 else 0 end as dsd_loan_page_show_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck = 1 then 1 else 0 end as dsd_loan_confirm_button_ck_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_input_pay_password_popup = 1 then 1 else 0 end as dsd_input_pay_password_popup_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_face_rec =1  then 1 else 0 end as dsd_face_rec_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_apply_submit_api_done =1 then 1 else 0 end as dsd_loan_apply_submit_api_done_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_face_rec * dsd_input_pay_password_popup > 0 then 1 else 0 end as check_both_tag,
                case when 
                    dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck  > 0 
                    and dsd_input_pay_password_popup > 0
                    and dsd_face_rec = 0
                then 1 else 0 end as check_password_tag,

                case when 
                    dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck  > 0 
                    and dsd_input_pay_password_popup = 0
                    and dsd_face_rec > 0
                then 1 else 0 end as check_face_tag

            from 
                (
                    select
                        tel,server_date,
                        count(distinct case when dsd_market_biopen=1 then tel else null end) as dsd_market_biopen,
                        count(distinct case when dsd_market_setup_sw=1 then tel else null end) as dsd_market_setup_sw,
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
                                dsd_loan_page_show,
                                dsd_loan_confirm_button_ck,
                                dsd_input_pay_password_popup,
                                dsd_face_rec,
                                dsd_loan_apply_submit_api_done,
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
                                            eventid in ('face_rec_button'
                                            --,'face_id_enter_button_result','face_check_face_result_success','fe_face_recognize_res'
                                            )
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
                                        concat_ws('-', year, month, day) between '${start_date}' and '${end_date}'
                                        and eventid in (
                                            'market_biopen',
                                            'market_setup_sw',
                                            'loan_page_show',
                                            'loan_confirm_button_ck',
                                            'input_pay_password_popup',
                                            'fe_input_pay_password_set_pwd_show',
                                            'fe_input_pay_password_close',
                                            'face_rec_button',
                                            'call_face_recognize_result',
                                            'face_id_enter_button_result',
                                            'face_check_face_result_success',
                                            'fe_face_recognize_res',
                                            'loan_apply_submit_api_done'
                                        )
                                ) t0 
                            where 
                                to_date(server_time) between '${start_date}' and '${end_date}'
                            group by 
                                tel, 
                                eventid,
                                dsd_market_biopen,
                                dsd_market_setup_sw,
                                dsd_loan_page_show,
                                dsd_loan_confirm_button_ck,
                                dsd_input_pay_password_popup,
                                dsd_face_rec,
                                dsd_loan_apply_submit_api_done,
                                to_date(server_time)
                        ) t0 
                    group by 
                        tel,server_date
                ) t0 
        ) t0 

        left outer join 
        (
            
            select
                uid,
                cell as tel 
            from 
                pbs_dw.dwv_passport_users -- 全量用户信息表
            where 
                concat_ws('-', year, month, day) = '${end_date}'
                and cell is not null
            group by uid, cell

        ) t1 
        on t0.tel = t1.tel
    where dsd_market_biopen_tag > 0

) t0 

left outer join 
(
    select
        uid, funds_channel_id
    from 
    (
        select
            uid, funds_channel_id, row_number() over(partition by uid order by success_time desc ) as rk
        from 
            manhattan_dw.dwd_loan_user_credit_d_whole
        where 
            concat_ws('-', year, month, day) = '${start_date}'
            and success_time like '20%'
            and status = 1
            and funds_channel_id like '30%'
    ) t0 
    where rk = 1
) t1
on t0.uid  = t1.uid 
"
wait

hive -e "
drop table if exists manhattan_test.dsd_user_loan_demand_samples_label_${start}_${end};
create table manhattan_test.dsd_user_loan_demand_samples_label_${start}_${end} as 
    select 
        uid, tel, funds_channel_id, server_date, dsd_market_biopen_tag, dsd_loan_confirm_button_ck_tag, 
        case when dsd_loan_confirm_button_ck_tag = 1 then 1 else 0 end as label
    from 
        manhattan_test.dsd_user_loan_demand_samples_${start}_${end}
    where 
        dsd_market_biopen_tag = 1

"
