===================== 分析各个主要资金方最近一段时间，用户在支用环节的埋点数据漏斗链路转化 ============================
链路转化
    step1：用户进入贷款服务页
        其余操作：用户在贷款服务页面点击”我的页面“，查看”我的借款“等信息操作

    step2：用户进入支用页面
        其余操作：用户输入金额，查看还款计划，借款周期等等

    step3：用户点击支用申请按钮
        其余操作：验证环节，主要包括密码与人脸两种

    step4：支用申请成功提交

    前端维护天眼埋点表
    manhattan_webapp.raven_loan_webapp_external_table

========================================= dsd 整体漏斗转化指标 ================================================

select 
    server_date, 
    sum(dsd_market_biopen_tag) as dsd_market_biopen_tag_cnt,
    sum(dsd_market_setup_sw_tag) as dsd_market_setup_sw_tag_cnt,
    sum(dsd_loan_page_show_tag) as dsd_loan_page_show_tag_cnt,
    sum(dsd_loan_confirm_button_ck_tag) as dsd_loan_confirm_button_ck_tag_cnt,
    sum(dsd_input_pay_password_popup_tag) as dsd_input_pay_password_popup_tag_cnt,
    sum(dsd_face_rec_tag) as dsd_face_rec_tag_cnt,
    sum(dsd_loan_apply_submit_api_done_tag) as dsd_loan_apply_submit_api_done_tag_cnt

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
        case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_apply_submit_api_done =1 then 1 else 0 end as dsd_loan_apply_submit_api_done_tag
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
                                concat_ws('-', year, month, day) >= '2020-07-01'
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
                        to_date(server_time) >= '2020-07-01'
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
group by server_date
order by server_date desc 


===================================== 分资方 整体漏斗转化指标 ======================================

select
    funds_channel_id,
    sum(dsd_market_biopen_tag) as dsd_market_biopen_tag_cnt,
    sum(dsd_market_setup_sw_tag) as dsd_market_setup_sw_tag_cnt,
    sum(dsd_loan_page_show_tag) as dsd_loan_page_show_tag_cnt,
    sum(dsd_loan_confirm_button_ck_tag) as dsd_loan_confirm_button_ck_tag_cnt,
    sum(dsd_input_pay_password_popup_tag) as dsd_input_pay_password_popup_tag_cnt,
    sum(dsd_face_rec_tag) as dsd_face_rec_tag_cnt,
    sum(dsd_loan_apply_submit_api_done_tag) as dsd_loan_apply_submit_api_done_tag_cnt
from 
(
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
                                            concat_ws('-', year, month, day) >= '2020-07-01'
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
                                    to_date(server_time) >= '2020-07-01'
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
                    tel, attrs['uid'] as uid, 
                    to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
                from    
                    manhattan_ods.ods_buddy_tracker
                where 
                    concat_ws('-', year, month, day) >= '2020-07-01'
                    and eventid = 'market_biopen' 
                    and attrs['funds_credited_list'] like '30%'
                    and attrs['page_name'] = 'market_home_page'
                group by  
                    tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
            ) t1 
            on t0.tel = t1.tel and t0.server_date = t1.server_date
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
                concat(year, month, day) = '20200715'
                and success_time like '20%'
                and status = 1
                and funds_channel_id like '30%'
        ) t0 
        where rk = 1
    ) t1
    on t0.uid  = t1.uid 
) t0 
group by 
    funds_channel_id
order by 
    dsd_market_biopen_tag_cnt desc 

============================= 滴水贷用户验证环节流失 =======================

select
    funds_channel_id, check_method,
    sum(dsd_loan_confirm_button_ck_tag) as dsd_loan_confirm_button_ck_cnt,
    sum(dsd_loan_apply_submit_api_done_tag) as dsd_loan_apply_submit_api_done_cnt
from 
(
    select
        t0.uid,
        t0.dsd_loan_confirm_button_ck_tag,
        t0.check_both_tag,
        t0.check_password_tag,
        t0.check_face_tag,
        t0.dsd_loan_apply_submit_api_done_tag,
        t1.funds_channel_id,
        case 
            when check_both_tag = 1 then 1 
            when check_password_tag = 1 then 2
            when check_face_tag = 1 then 3
        else 0 end as check_method
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
                                                concat_ws('-', year, month, day) >= '2020-07-01'
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
                                        to_date(server_time) >= '2020-07-01'
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
                        tel, attrs['uid'] as uid, 
                        to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
                    from    
                        manhattan_ods.ods_buddy_tracker
                    where 
                        concat_ws('-', year, month, day) >= '2020-07-01'
                        and eventid = 'market_biopen' 
                        and attrs['funds_credited_list'] like '30%'
                        and attrs['page_name'] = 'market_home_page'
                    group by  
                        tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
                ) t1 
                on t0.tel = t1.tel and t0.server_date = t1.server_date
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
                    concat(year, month, day) = '20200715'
                    and success_time like '20%'
                    and status = 1
                    and funds_channel_id like '30%'
            ) t0 
            where rk = 1
        ) t1
        on t0.uid  = t1.uid 
    where
        dsd_loan_confirm_button_ck_tag>0
) t0 
group by 
    funds_channel_id, check_method
order by 
    dsd_loan_confirm_button_ck_cnt, check_method


====================== 自营问题case ==========================

select
    t0.uid,
    t0.dsd_loan_confirm_button_ck_tag,
    t0.dsd_loan_apply_submit_api_done_tag,
    t1.funds_channel_id
from 
(
    select
        t0.*,
        t1.uid
    from 
        (
            select 
                tel,server_date,
                case when dsd_market_biopen>0 then 1 else 0 end as dsd_market_biopen_tag,
                case when dsd_market_biopen * dsd_market_setup_sw >0 then 1 else 0 end as dsd_market_setup_sw_tag,
                case when dsd_market_biopen * dsd_loan_page_show >0 then 1 else 0 end as dsd_loan_page_show_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck >0 then 1 else 0 end as dsd_loan_confirm_button_ck_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_input_pay_password_popup >0 then 1 else 0 end as dsd_input_pay_password_popup_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_face_rec >0  then 1 else 0 end as dsd_face_rec_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_apply_submit_api_done >0 then 1 else 0 end as dsd_loan_apply_submit_api_done_tag,
                case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck * dsd_face_rec * dsd_input_pay_password_popup > 0 then 1 else 0 end as check_both_tag
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
                                        concat_ws('-', year, month, day) >= '2020-07-01'
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
                                to_date(server_time) = '2020-07-15'
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
                tel, attrs['uid'] as uid, 
                to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
            from    
                manhattan_ods.ods_buddy_tracker
            where 
                concat_ws('-', year, month, day) = '2020-07-15'
                and eventid = 'market_biopen' 
                and attrs['funds_credited_list'] like '30%'
                and attrs['page_name'] = 'market_home_page'
            group by  
                tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
        ) t1 
        on t0.tel = t1.tel and t0.server_date = t1.server_date
    where
        dsd_market_biopen_tag>0
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
            concat(year, month, day) = '20200715'
            and success_time like '20%'
            and status = 1
            and funds_channel_id like '30%'
    ) t0 
    where rk = 1
) t1
on t0.uid  = t1.uid 
where 
    t1.funds_channel_id in ('3019','3023','3026')
    and t0.dsd_loan_confirm_button_ck_tag > 0
    and t0.dsd_loan_apply_submit_api_done_tag = 0 
limit 100

=============================== 贷超漏斗转化指标 ==============================

select
    server_date,
    sum(market_biopen) as market_biopen,
    sum(market_setup_sw) as market_setup_sw,
    sum(market_myloan_sw) as market_myloan_sw,
    sum(market_myloan_detail_check) as market_myloan_detail_check,
    sum(market_myloan_confirm_ck) as market_myloan_confirm_ck,
    sum(market_input_pay_password_popup) as market_input_pay_password_popup,
    sum(market_loan_face_check) as market_loan_face_check,
    sum(market_loan_result_sw) as market_loan_result_sw
from 
(
    select
        t0.*, 
        t1.uid
    from 
        (
            select 
                tel,server_date,
                case when market_biopen> 0 then 1 else 0 end as market_biopen,
                case when market_biopen * market_setup_sw > 0 then 1 else 0 end as market_setup_sw,
                case when market_biopen * market_myloan_sw > 0 then 1 else 0 end as market_myloan_sw,
                case when market_biopen * market_myloan_sw * market_myloan_detail_check > 0 then 1 else 0 end as market_myloan_detail_check,
                case when market_biopen * market_myloan_sw * market_myloan_confirm_ck > 0 then 1 else 0 end as market_myloan_confirm_ck,
                case when market_biopen * market_myloan_sw * market_myloan_confirm_ck * market_input_pay_password_popup > 0 then 1 else 0 end as market_input_pay_password_popup,
                case when market_biopen * market_myloan_sw * market_myloan_confirm_ck * market_loan_face_check > 0 then 1 else 0 end as market_loan_face_check,
                case when market_biopen * market_myloan_sw * market_myloan_confirm_ck * market_loan_result_sw > 0 then 1 else 0 end as market_loan_result_sw
            from 
                (
                    select
                        tel,server_date,
                        count(distinct case when market_biopen=1 then tel else null end) as market_biopen,
                        count(distinct case when market_setup_sw=1 then tel else null end) as market_setup_sw,
                        count(distinct case when market_myloan_sw=1 then tel else null end) as market_myloan_sw,
                        count(distinct case when market_myloan_detail_check=1 then tel else null end) as market_myloan_detail_check,
                        count(distinct case when market_myloan_confirm_ck=1 then tel else null end) as market_myloan_confirm_ck,
                        count(distinct case when market_input_pay_password_popup=1 then tel else null end) as market_input_pay_password_popup,
                        count(distinct case when market_loan_face_check=1 then tel else null end) as market_loan_face_check,
                        count(distinct case when market_loan_result_sw=1 then tel else null end) as market_loan_result_sw

                    from 
                        (
                            select
                                tel, 
                                eventid,
                                market_biopen,
                                market_setup_sw,
                                market_myloan_sw,
                                market_myloan_detail_check,
                                market_myloan_confirm_ck,
                                market_input_pay_password_popup,
                                market_loan_face_check,
                                market_loan_result_sw,
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
                                            and attrs['page_name'] = 'market_home_page'
                                            then 1 else 0 
                                        end as market_biopen,

                                        case when 
                                            eventid = 'market_setup_sw' 
                                            then 1 else 0 
                                        end as market_setup_sw,

                                        case when 
                                            eventid ='market_myloan_sw'
                                            then 1 else 0 
                                        end as market_myloan_sw,

                                        case when 
                                            eventid in ( 
                                            'market_myloan_duration_ck',
                                            'market_myloan_duration_sw',
                                            'market_myloan_durselect_ck',
                                            'market_myloan_repay_plan_ck',
                                            'market_myloan_repay_plan_sw')
                                            then 1 else 0 
                                        end as market_myloan_detail_check,

                                        case when 
                                            eventid = 'market_myloan_confirm_ck'
                                            then 1 else 0 
                                        end as market_myloan_confirm_ck,

                                        case when 
                                            eventid = 'market_input_pay_password_popup'
                                            then 1 else 0
                                        end as market_input_pay_password_popup,

                                        case when 
                                            eventid in (
                                            'market_loan_faceguid_sw',
                                            'market_loan_facereco_ck',
                                            'market_apply_facerecog_confirm_sw'
                                            )
                                            then 1 else 0 
                                        end as market_loan_face_check,

                                        case when 
                                            eventid = 'market_loan_result_sw'
                                            then 1 else 0 
                                        end as market_loan_result_sw

                                    from
                                        manhattan_ods.ods_buddy_tracker
                                    where 
                                        concat_ws('-', year, month, day) >= '2020-07-01'
                                        and eventid in (
                                            'market_biopen',
                                            'market_setup_sw',
                                            'market_myloan_sw',
                                            'market_myloan_duration_ck',
                                            'market_myloan_duration_sw',
                                            'market_myloan_durselect_ck',
                                            'market_myloan_repay_plan_ck',
                                            'market_myloan_repay_plan_sw',
                                            'market_myloan_confirm_ck',
                                            'market_input_pay_password_popup',
                                            'market_loan_faceguid_sw',
                                            'market_loan_facereco_ck',
                                            'market_apply_facerecog_confirm_sw',
                                            'market_loan_result_sw'
                                        )
                                ) t0 
                            where 
                                to_date(server_time) >= '2020-07-01'
                            group by 
                                tel, 
                                eventid,
                                market_biopen,
                                market_setup_sw,
                                market_myloan_sw,
                                market_myloan_detail_check,
                                market_myloan_confirm_ck,
                                market_input_pay_password_popup,
                                market_loan_face_check,
                                market_loan_result_sw,
                                to_date(server_time)
                        ) t0 
                    group by 
                        tel,server_date
                ) t0 
        ) t0 

        join 
        (
            select
                tel, attrs['uid'] as uid, 
                to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
            from    
                manhattan_ods.ods_buddy_tracker
            where 
                concat_ws('-', year, month, day) >= '2020-07-01'
                and eventid = 'market_biopen' 
                and (attrs['funds_credited_list'] like '36%' or attrs['funds_distrib_list'] like '36%')
                and attrs['page_name'] = 'market_home_page'
                and to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) >= '2020-07-01'
            group by  
                tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
        ) t1 
        on t0.tel = t1.tel and t0.server_date = t1.server_date
    where market_biopen > 0
) t0 
group by 
    server_date



============================= 用户在进入滴水贷之后的贷款服务页是否对进入支用页面的关系 ==========================
1 使用buddy tracker表
select
    funds_channel_id,
    dsd_mycoupon_ck_tag,
    dsd_mine_repayment_entry_ck_tag,
    dsd_loan_list_sw_tag,
    sum(dsd_market_biopen_tag) as dsd_market_biopen_tag_cnt,
    sum(dsd_loan_page_show_tag) as dsd_loan_page_show_tag_cnt,
    sum(dsd_loan_confirm_button_ck_tag) as dsd_loan_confirm_button_ck_tag_cnt
from 
    (
        select
            t0.*, 
            t1.funds_channel_id
        from 
        (
            select
                t0.*,
                t1.uid
            from 
                (
                    select 
                        tel,server_date,
                        case when dsd_market_biopen>0 then 1 else 0 end as dsd_market_biopen_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw >0 then 1 else 0 end as dsd_market_setup_sw_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_mycoupon_ck>0 then 1 else 0 end as dsd_mycoupon_ck_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_mine_repayment_entry_ck>0 then 1 else 0 end as dsd_mine_repayment_entry_ck_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_loan_list_sw>0 then 1 else 0 end as dsd_loan_list_sw_tag,
                        case when dsd_market_biopen * dsd_loan_page_show>0 then 1 else 0 end as dsd_loan_page_show_tag,
                        case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck>0 then 1 else 0 end as dsd_loan_confirm_button_ck_tag
                    from 
                        (
                            select
                                tel,server_date,
                                count(distinct case when dsd_market_biopen=1 then tel else null end) as dsd_market_biopen,
                                count(distinct case when dsd_market_setup_sw=1 then tel else null end) as dsd_market_setup_sw,
                                count(distinct case when dsd_mycoupon_ck=1 then tel else null end) as dsd_mycoupon_ck,
                                count(distinct case when dsd_mine_repayment_entry_ck=1 then tel else null end) as dsd_mine_repayment_entry_ck,
                                count(distinct case when dsd_loan_list_sw=1 then tel else null end) as dsd_loan_list_sw,
                                count(distinct case when dsd_loan_page_show=1 then tel else null end) as dsd_loan_page_show,
                                count(distinct case when dsd_loan_confirm_button_ck=1 then tel else null end) as dsd_loan_confirm_button_ck
                            from 
                                (
                                    select
                                        tel, 
                                        eventid,
                                        dsd_market_biopen,
                                        dsd_market_setup_sw,
                                        dsd_mycoupon_ck,
                                        dsd_mine_repayment_entry_ck,
                                        dsd_loan_list_sw,
                                        dsd_loan_page_show,
                                        dsd_loan_confirm_button_ck,
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
                                                    eventid = 'market_mine_repayment_entry_ck'
                                                    and attrs['funds_credited_list'] like '30%'
                                                    then 1 else 0 
                                                end as dsd_mine_repayment_entry_ck,

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
                                                end as dsd_loan_confirm_button_ck

                                            from
                                                manhattan_ods.ods_buddy_tracker
                                            where 
                                                concat_ws('-', year, month, day) >= '2020-07-01'
                                                and eventid in (
                                                    'market_biopen',
                                                    'market_setup_sw',
                                                    'loan_page_show',
                                                    'loan_confirm_button_ck',
                                                    'market_setup_mycoupon_ck',
                                                    'setup_mycoupon_sk',
                                                    'market_mine_repayment_entry_ck',
                                                    'market_loan_list_sw'
                                                )
                                        ) t0 
                                    where 
                                        to_date(server_time) >= '2020-07-01'
                                    group by 
                                        tel, 
                                        eventid,
                                        dsd_market_biopen,
                                        dsd_market_setup_sw,
                                        dsd_mycoupon_ck,
                                        dsd_mine_repayment_entry_ck,
                                        dsd_loan_list_sw,
                                        dsd_loan_page_show,
                                        dsd_loan_confirm_button_ck,
                                        to_date(server_time)
                                ) t0 
                            group by 
                                tel,server_date
                        ) t0 
                ) t0 

                left outer join 
                (
                    select
                        tel, attrs['uid'] as uid, 
                        to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
                    from    
                        manhattan_ods.ods_buddy_tracker
                    where 
                        concat_ws('-', year, month, day) >= '2020-07-01'
                        and eventid = 'market_biopen' 
                        and attrs['funds_credited_list'] like '30%'
                        and attrs['page_name'] = 'market_home_page'
                    group by  
                        tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
                ) t1 
                on t0.tel = t1.tel and t0.server_date = t1.server_date
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
                    concat(year, month, day) = '20200715'
                    and success_time like '20%'
                    and status = 1
                    and funds_channel_id like '30%'
            ) t0 
            where rk = 1
        ) t1
        on t0.uid  = t1.uid 
    ) t0 
group by 
    funds_channel_id,
    dsd_mycoupon_ck_tag,
    dsd_mine_repayment_entry_ck_tag,
    dsd_loan_list_sw_tag
order by 
    dsd_loan_page_show_tag_cnt desc 

2.使用天眼埋点表
select
    funds_channel_id,
    dsd_mycoupon_ck_tag,
    dsd_mine_repayment_entry_ck_tag,
    dsd_loan_list_sw_tag,
    sum(dsd_market_biopen_tag) as dsd_market_biopen_tag_cnt,
    sum(dsd_loan_page_show_tag) as dsd_loan_page_show_tag_cnt,
    sum(dsd_loan_confirm_button_ck_tag) as dsd_loan_confirm_button_ck_tag_cnt
from 
    (
        select
            t0.*, 
            t1.funds_channel_id
        from 
        (
            select
                t0.*,
                t1.uid
            from 
                (
                    select 
                        tel,server_date,
                        case when dsd_market_biopen>0 then 1 else 0 end as dsd_market_biopen_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw >0 then 1 else 0 end as dsd_market_setup_sw_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_mycoupon_ck>0 then 1 else 0 end as dsd_mycoupon_ck_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_mine_repayment_entry_ck>0 then 1 else 0 end as dsd_mine_repayment_entry_ck_tag,
                        case when dsd_market_biopen * dsd_market_setup_sw * dsd_loan_list_sw>0 then 1 else 0 end as dsd_loan_list_sw_tag,
                        case when dsd_market_biopen * dsd_loan_page_show>0 then 1 else 0 end as dsd_loan_page_show_tag,
                        case when dsd_market_biopen * dsd_loan_page_show * dsd_loan_confirm_button_ck>0 then 1 else 0 end as dsd_loan_confirm_button_ck_tag
                    from 
                        (
                            select
                                tel,server_date,
                                count(distinct case when dsd_market_biopen=1 then tel else null end) as dsd_market_biopen,
                                count(distinct case when dsd_market_setup_sw=1 then tel else null end) as dsd_market_setup_sw,
                                count(distinct case when dsd_mycoupon_ck=1 then tel else null end) as dsd_mycoupon_ck,
                                count(distinct case when dsd_mine_repayment_entry_ck=1 then tel else null end) as dsd_mine_repayment_entry_ck,
                                count(distinct case when dsd_loan_list_sw=1 then tel else null end) as dsd_loan_list_sw,
                                count(distinct case when dsd_loan_page_show=1 then tel else null end) as dsd_loan_page_show,
                                count(distinct case when dsd_loan_confirm_button_ck=1 then tel else null end) as dsd_loan_confirm_button_ck
                            from 
                                (
                                    select
                                        tel, 
                                        eventid,
                                        dsd_market_biopen,
                                        dsd_market_setup_sw,
                                        dsd_mycoupon_ck,
                                        dsd_mine_repayment_entry_ck,
                                        dsd_loan_list_sw,
                                        dsd_loan_page_show,
                                        dsd_loan_confirm_button_ck,
                                        to_date(server_time) as server_date
                                    from 
                                        (
                                            select 
                                                tel, 
                                                eventid, 
                                                from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)) as server_time,
                                                --- concat_ws('-', year, month, day) as pt,
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
                                                    eventid = 'market_mine_repayment_entry_ck'
                                                    and attrs['funds_credited_list'] like '30%'
                                                    then 1 else 0 
                                                end as dsd_mine_repayment_entry_ck,

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
                                                end as dsd_loan_confirm_button_ck

                                            from
                                                manhattan_webapp.raven_loan_webapp_external_table
                                            where 
                                                --- concat_ws('-', year, month, day) >= '2020-07-01' and hour>=0
                                                and year = 2020 and month = 7 and day between 1 and 20 
                                                and eventid in (
                                                    'market_biopen',
                                                    'market_setup_sw',
                                                    'loan_page_show',
                                                    'loan_confirm_button_ck',
                                                    'market_setup_mycoupon_ck',
                                                    'setup_mycoupon_sk',
                                                    'market_mine_repayment_entry_ck',
                                                    'market_loan_list_sw'
                                                )
                                        ) t0 
                                    where 
                                        to_date(server_time) >= '2020-07-01'
                                    group by 
                                        tel, 
                                        eventid,
                                        dsd_market_biopen,
                                        dsd_market_setup_sw,
                                        dsd_mycoupon_ck,
                                        dsd_mine_repayment_entry_ck,
                                        dsd_loan_list_sw,
                                        dsd_loan_page_show,
                                        dsd_loan_confirm_button_ck,
                                        to_date(server_time)
                                ) t0 
                            group by 
                                tel,server_date
                        ) t0 
                ) t0 

                left outer join 
                (
                    select
                        tel, attrs['uid'] as uid, 
                        to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
                    from    
                        manhattan_ods.ods_buddy_tracker
                    where 
                        concat_ws('-', year, month, day) >= '2020-07-01'
                        and eventid = 'market_biopen' 
                        and attrs['funds_credited_list'] like '30%'
                        and attrs['page_name'] = 'market_home_page'
                    group by  
                        tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
                ) t1 
                on t0.tel = t1.tel and t0.server_date = t1.server_date
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
                    concat(year, month, day) = '20200715'
                    and success_time like '20%'
                    and status = 1
                    and funds_channel_id like '30%'
            ) t0 
            where rk = 1
        ) t1
        on t0.uid  = t1.uid 
    ) t0 
group by 
    funds_channel_id,
    dsd_mycoupon_ck_tag,
    dsd_mine_repayment_entry_ck_tag,
    dsd_loan_list_sw_tag
order by 
    dsd_loan_page_show_tag_cnt desc 

======================== 用户从贷款服务页进入到支用页面的人群分析 ==============================

create table manhattan_test.dsd_user_eventid_info_20200701_20200720 as 
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
                                        concat_ws('-', year, month, day) >= '2020-07-01'
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
                                to_date(server_time) >= '2020-07-01'
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
                tel, attrs['uid'] as uid, 
                to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint))) as server_date
            from    
                manhattan_ods.ods_buddy_tracker
            where 
                concat_ws('-', year, month, day) >= '2020-07-01'
                and eventid = 'market_biopen' 
                and attrs['funds_credited_list'] like '30%'
                and attrs['page_name'] = 'market_home_page'
            group by  
                tel, attrs['uid'], to_date(from_unixtime( cast(substr(cast(client_time as string),1,10) as bigint)))
        ) t1 
        on t0.tel = t1.tel and t0.server_date = t1.server_date
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
            concat(year, month, day) = '20200715'
            and success_time like '20%'
            and status = 1
            and funds_channel_id like '30%'
    ) t0 
    where rk = 1
) t1
on t0.uid  = t1.uid 


=========================== 样本预测贷款服务页->点击支用转化 ==========================

select
    funds_channel_id, label, count(distinct uid, server_date)
from 
(
    select 
        uid, funds_channel_id, server_date, dsd_market_biopen_tag, dsd_loan_confirm_button_ck_tag, 
        case when dsd_loan_confirm_button_ck_tag = 1 then 1 else 0 end as label
    from 
        manhattan_test.dsd_user_eventid_info_20200701_20200720
    where 
        dsd_market_biopen_tag = 1
) t0 
group by funds_channel_id,label 
order by funds_channel_id,label 
