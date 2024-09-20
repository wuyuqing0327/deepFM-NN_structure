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
drop table if exists  manhattan_test.user_demand_model_event_stat_fea_${start_pt}_${end_pt};
create table manhattan_test.user_demand_model_event_stat_fea_${start_pt}_${end_pt} as 
select
    t0.uid, t0.funds_channel_id, t0.server_date, t0.label,

    ---- market_biopen
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) > 0 then 1 else 0 end as is_last_1d_market_biopen,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as last_1d_market_biopen_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_biopen', 1,0)) > 0 then 1 else 0 end as is_last_3d_market_biopen,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_biopen', 1,0)) as last_3d_market_biopen_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_biopen', 1,0)) > 0 then 1 else 0 end as is_last_7d_market_biopen,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_biopen', 1,0)) as last_7d_market_biopen_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) > 0 then 1 else 0 end as is_last_1m_market_biopen,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as last_1m_market_biopen_cnt,
    datediff(t0.server_date, max(case when eventid = 'market_biopen' then server_time else add_months(t0.server_date, -1) end)) as days_from_last_market_biopen,

    ---- market_setup_sw
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_setup_sw', 1,0)) > 0 then 1 else 0 end as is_last_1d_market_setup_sw,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_setup_sw', 1,0)) as last_1d_market_setup_sw_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_setup_sw', 1,0)) > 0 then 1 else 0 end as is_last_3d_market_setup_sw,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_setup_sw', 1,0)) as last_3d_market_setup_sw_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_setup_sw', 1,0)) > 0 then 1 else 0 end as is_last_7d_market_setup_sw,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_setup_sw', 1,0)) as last_7d_market_setup_sw_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_setup_sw', 1,0)) > 0 then 1 else 0 end as is_last_1m_market_setup_sw,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_setup_sw', 1,0)) as last_1m_market_setup_sw_cnt,
    datediff(t0.server_date, max(case when eventid = 'market_setup_sw' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_setup_sw,

    --- mycoupon_ck
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) > 0 then 1 else 0 end as is_last_1d_mycoupon_ck,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) as last_1d_mycoupon_ck_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) > 0 then 1 else 0 end as is_last_3d_mycoupon_ck,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) as last_3d_mycoupon_ck_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) > 0 then 1 else 0 end as is_last_7d_mycoupon_ck,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) as last_7d_mycoupon_ck_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) > 0 then 1 else 0 end as is_last_1m_mycoupon_ck,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1,0)) as last_1m_mycoupon_ck_cnt,
    datediff(t0.server_date, max(case when eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk') then server_time else add_months(t0.server_date, -1) end )) as days_from_last_mycoupon_ck,

    --- market_loan_list_sw
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_loan_list_sw', 1,0)) > 0 then 1 else 0 end as is_last_1d_market_loan_list_sw,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'market_loan_list_sw', 1,0)) as last_1d_market_loan_list_sw_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_loan_list_sw', 1,0)) > 0 then 1 else 0 end as is_last_3d_market_loan_list_sw,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'market_loan_list_sw', 1,0)) as last_3d_market_loan_list_sw_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_loan_list_sw', 1,0)) > 0 then 1 else 0 end as is_last_7d_market_loan_list_sw,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'market_loan_list_sw', 1,0)) as last_7d_market_loan_list_sw_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_loan_list_sw', 1,0)) > 0 then 1 else 0 end as is_last_1m_market_loan_list_sw,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_loan_list_sw', 1,0)) as last_1m_market_loan_list_sw_cnt,
    datediff(t0.server_date, max(case when eventid = 'market_loan_list_sw' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_loan_list_sw,

    --- loan_page_show
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) > 0 then 1 else 0 end as is_last_1d_loan_page_show,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) as last_1d_loan_page_show_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_page_show', 1,0)) > 0 then 1 else 0 end as is_last_3d_loan_page_show,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_page_show', 1,0)) as last_3d_loan_page_show_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_page_show', 1,0)) > 0 then 1 else 0 end as is_last_7d_loan_page_show,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_page_show', 1,0)) as last_7d_loan_page_show_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) > 0 then 1 else 0 end as is_last_1m_loan_page_show,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) as last_1m_loan_page_show_cnt,
    datediff(t0.server_date, max(case when eventid = 'loan_page_show' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_loan_page_show,

    --- loan_confirm_button_ck
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1,0)) > 0 then 1 else 0 end as is_last_1d_loan_confirm_button_ck,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1,0)) as last_1d_loan_confirm_button_ck_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_confirm_button_ck', 1,0)) > 0 then 1 else 0 end as is_last_3d_loan_confirm_button_ck,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_confirm_button_ck', 1,0)) as last_3d_loan_confirm_button_ck_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_confirm_button_ck', 1,0)) > 0 then 1 else 0 end as is_last_7d_loan_confirm_button_ck,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_confirm_button_ck', 1,0)) as last_7d_loan_confirm_button_ck_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1,0)) > 0 then 1 else 0 end as is_last_1m_loan_confirm_button_ck,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1,0)) as last_1m_loan_confirm_button_ck_cnt,
    datediff(t0.server_date, max(case when eventid = 'loan_confirm_button_ck' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_loan_confirm_button_ck,

    --- input_pay_password_popup
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'input_pay_password_popup', 1,0)) > 0 then 1 else 0 end as is_last_1d_input_pay_password_popup,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'input_pay_password_popup', 1,0)) as last_1d_input_pay_password_popup_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'input_pay_password_popup', 1,0)) > 0 then 1 else 0 end as is_last_3d_input_pay_password_popup,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'input_pay_password_popup', 1,0)) as last_3d_input_pay_password_popup_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'input_pay_password_popup', 1,0)) > 0 then 1 else 0 end as is_last_7d_input_pay_password_popup,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'input_pay_password_popup', 1,0)) as last_7d_input_pay_password_popup_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'input_pay_password_popup', 1,0)) > 0 then 1 else 0 end as is_last_1m_input_pay_password_popup,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'input_pay_password_popup', 1,0)) as last_1m_input_pay_password_popup_cnt,
    datediff(t0.server_date, max(case when eventid = 'input_pay_password_popup' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_input_pay_password_popup,

    ---- face_rec_button
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'face_rec_button', 1,0)) > 0 then 1 else 0 end as is_last_1d_face_rec_button,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'face_rec_button', 1,0)) as last_1d_face_rec_button_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'face_rec_button', 1,0)) > 0 then 1 else 0 end as is_last_3d_face_rec_button,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'face_rec_button', 1,0)) as last_3d_face_rec_button_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'face_rec_button', 1,0)) > 0 then 1 else 0 end as is_last_7d_face_rec_button,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'face_rec_button', 1,0)) as last_7d_face_rec_button_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'face_rec_button', 1,0)) > 0 then 1 else 0 end as is_last_1m_face_rec_button,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'face_rec_button', 1,0)) as last_1m_face_rec_button_cnt,
    datediff(t0.server_date, max(case when eventid = 'face_rec_button' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_market_face_rec_button,

    ---- loan_apply_submit_api_done
    case when sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1,0)) > 0 then 1 else 0 end as is_last_1d_loan_apply_submit_api_done,
    sum(if( t1.server_date = date_add(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1,0)) as last_1d_loan_apply_submit_api_done_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_apply_submit_api_done', 1,0)) > 0 then 1 else 0 end as is_last_3d_loan_apply_submit_api_done,
    sum(if( t1.server_date >= date_add(t0.server_date, -3) and eventid = 'loan_apply_submit_api_done', 1,0)) as last_3d_loan_apply_submit_api_done_cnt,
    case when sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_apply_submit_api_done', 1,0)) > 0 then 1 else 0 end as is_last_7d_loan_apply_submit_api_done,
    sum(if( t1.server_date >= date_add(t0.server_date, -7) and eventid = 'loan_apply_submit_api_done', 1,0)) as last_7d_loan_apply_submit_api_done_cnt,
    case when sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1,0)) > 0 then 1 else 0 end as is_last_1m_loan_apply_submit_api_done,
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1,0)) as last_1m_loan_apply_submit_api_done_cnt,
    datediff(t0.server_date, max(case when eventid = 'loan_apply_submit_api_done' then server_time else add_months(t0.server_date, -1) end )) as days_from_last_loan_apply_submit_api_done,

    --- from market_biopen to loan_page_show ratio during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_1,

    --- from loan_page_show to loan_confirm_button_ck ratio during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) as ratio_2,

    --- from market_biopen to mycoupon_ck ratio during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid in ('market_setup_mycoupon_ck','setup_mycoupon_sk'), 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_3,

    --- from market_biopen to market_loan_list_sw during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_loan_list_sw', 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_4,

    --- from market_biopen to market_setup_sw during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_setup_sw', 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_5,

    --- from market_biopen to loan_confirm_button_ck during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_confirm_button_ck', 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_6,

    --- from loan_page_show to loan_apply_submit_api_done during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_page_show', 1,0)) as ratio_7,

    --- from market_biopen to loan_apply_submit_api_done during last month
    sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'loan_apply_submit_api_done', 1 ,0)) / sum(if( t1.server_date >= add_months(t0.server_date, -1) and eventid = 'market_biopen', 1,0)) as ratio_8

from
    (
        select uid, funds_channel_id, tel, label, server_date
        from manhattan_test.dsd_user_loan_demand_samples_label_${start_pt}_${end_pt}
        where funds_channel_id is not null 
    ) t0 
    left outer join 

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
                    concat_ws('-', year, month, day) between add_months('${start_date}', -1) and '${end_date}'
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
            to_date(server_time) between add_months('${start_date}', -1) and '${end_date}'
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
    ) t1
    on t0.tel = t1.tel
where
    t1.server_date >= add_months(t0.server_date, -1) 
    and t1.server_date < t0.server_date
group by 
    t0.uid, t0.funds_channel_id, t0.server_date, t0.label

"
