#!/bin/bash
source ~/.bash_profile

start_date=$1
end_date=$2

start_pt=`date -d"-0 day $start_date" '+%Y%m%d'`
end_pt=`date -d"-0 day $end_date" '+%Y%m%d'`

echo "start date: ${start_date}"
echo "end date: ${end_date}"
echo "start pt: ${start_pt}"
echo "end pt: ${end_pt}"

hive -e "

set hive.support.quoted.identifiers=none;

drop table if exists manhattan_test.dsd_user_loan_demand_xgb_stat_${start_pt}_${end_pt};
create table manhattan_test.dsd_user_loan_demand_xgb_stat_${start_pt}_${end_pt} as

select
    feat.uid,
    feat.server_date,
    \`(uid|server_date)?+.+\`
from

(
    select
        label.uid,
        label.server_date,

    -- feature_individual_manhattan_drip_credit
        current_drip_loan_credit_line_use_rate,
        current_drip_loan_credit_line_available,
    --     current_drip_credit_line_balance,
    --     current_drip_credit_line_use_rate,
        datediff(to_date(label.server_date),to_date(last_credit_success_end_time)) as last_credit_success_end_time,
        his_drip_max_credit_line,
        datediff(to_date(label.server_date),to_date(first_drip_apply_loan_pass_time)) as first_drip_apply_loan_pass_time,
        datediff(to_date(label.server_date),to_date(last_credit_success_start_time)) as last_credit_success_start_time,
        datediff(to_date(label.server_date),to_date(last_drip_apply_loan_time)) as last_drip_apply_loan_time,
        current_drip_interest_rate,
        last_drip_funds_channel_id,
        datediff(to_date(label.server_date),to_date(last_drip_apply_loan_pass_time)) as last_drip_apply_loan_pass_time,
        last_credit_identity_card_city_id,
        his_drip_min_credit_line,
        last_credit_mobile_city_id,
        datediff(to_date(label.server_date),to_date(last_bind_card_time)) as last_bind_card_time,
        last_credit_identity_card_user_age,
        total_drip_apply_loan_suc_funds_channel_num,
        current_drip_credit_line,

    -- feature_individual_manhattan_drip_repay
        datediff(to_date(label.server_date),to_date(last_drip_plan_end_time)) as last_drip_plan_end_time,
        datediff(to_date(label.server_date),to_date(last_drip_real_repay_time)) as last_drip_real_repay_time,
        datediff(to_date(label.server_date),to_date(current_drip_plan_end_time)) as current_drip_plan_end_time,
        last_30_day_drip_repay_money,
        datediff(to_date(label.server_date),to_date(last_drip_before_real_repay_time)) as last_drip_before_real_repay_time,
        last_30_day_drip_repay_principal,
        last_3_month_drip_repay_principal,
        last_7_day_drip_repay_principal,
        last_3_month_drip_repay_money,
        min_drip_mon_paid_principal,
        last_1_year_drip_before_repay_num,
        last_14_day_drip_repay_num,
        last_6_month_drip_repay_num,
        last_7_day_drip_repay_money,
        his_drip_single_three_days_num,
        last_14_day_drip_repay_money,
        last_14_day_drip_repay_principal,
        last_7_day_drip_before_repay_num,
        datediff(to_date(label.server_date),to_date(current_drip_real_end_time)) as current_drip_real_end_time,
        his_drip_repay_num,
        last_30_day_drip_weekend_repay_money,
        last_1_year_drip_repay_num,
        last_6_month_drip_before_repay_num,
        last_6_month_drip_single_three_days_num,
        current_drip_account_balanced_days,
        last_30_day_drip_before_paid_principal,
        last_3_month_drip_weekend_repay_money,
        last_7_day_drip_weekend_repay_money,

    -- feature_individual_manhattan_drip_disburse
        datediff(to_date(label.server_date),to_date(last_drip_disburse_ucc_time)) as last_drip_disburse_ucc_time,
        datediff(to_date(label.server_date),to_date(last_drip_disburse_failed_time)) as last_drip_disburse_failed_time,
        his_drip_repay_not_done_num,
        avg_drip_total_money,
        current_drip_in_loan_money,
        last_7_day_drip_loan_failed_num,
        last_3_month_drip_loan_failed_num,
        last_30_day_drip_max_loan_day_principal,
        last_drip_isburse_succ_principal,
        last_14_day_drip_loan_failed_num,
        datediff(to_date(label.server_date),to_date(second_drip_disburse_succ_time)) as second_drip_disburse_succ_time,
        last_7_day_drip_max_loan_day_principal,
        last_6_month_drip_loan_num,
        last_3_month_drip_loan_failed_money,
        last_3_month_drip_loan_num,
        first_drip_disburse_succ_principal,
        last_1_year_drip_loan_failed_money,
        avg_drip_mon_loan_money,
        his_drip_repay_done_num,
        last_6_month_drip_loan_failed_money,
        datediff(to_date(label.server_date),to_date(first_drip_disburse_failed_time)) as first_drip_disburse_failed_time,
        last_30_day_drip_loan_failed_num,
        last_1_year_drip_loan_failed_num,
        last_3_month_drip_max_loan_day_principal,
        last_14_day_drip_max_loan_day_principal,
        first_disburse,
        last_6_month_drip_loan_failed_num,
        last_1_year_drip_night_loan_num,
        last_7_day_drip_loan_failed_money,
        his_drip_loan_money,
        last_30_day_drip_loan_money,
        first_drip_disburse_succ_duration,
        last_1_year_drip_loan_num,
        last_6_month_drip_loan_money,
        last_30_day_drip_loan_failed_money,
        min_drip_total_duration,
        last_14_day_drip_loan_money,
        last_3_month_drip_loan_money,
        datediff(to_date(label.server_date),to_date(first_drip_disburse_succ_time)) as first_drip_disburse_succ_time,
        last_14_day_drip_loan_failed_money,
        last_6_month_drip_max_loan_day_principal,

    -- feature_individual_manhattan_drip_flow
        last_3_month_drip_num,
        his_face_num,
        last_6_month_drip_num,
        last_7_day_drip_num,
        last_1_year_drip_num,
        his_drip_num,
        datediff(to_date(label.server_date),to_date(last_face_submit_time)) as last_face_submit_time,
        his_drip_homepage_num,
        last_1_year_drip_loan_result_page_num,
        his_drip_loan_result_page_num,
        last_7_day_drip_loan_result_page_num,
        last_30_day_drip_num,
        last_6_month_drip_loan_result_page_num,
        last_30_day_drip_loan_result_page_num,
        last_3_month_drip_loan_result_page_num,
        his_drip_repayment_result_page_num,
        last_14_day_drip_repayment_result_page_num,
        last_14_day_drip_num,
        last_30_day_drip_repayment_button_num,
        last_14_day_drip_loan_result_page_num,
        last_14_day_drip_total_repayment_button_num,
        last_7_day_drip_total_repayment_button_num,
        last_3_month_drip_repayment_button_num,
        last_1_year_drip_total_repayment_button_num,
        his_drip_credit_contract_check_card_button_num,
        last_30_day_drip_repayment_result_page_num,
        last_3_month_drip_repayment_result_page_num,
        last_1_year_drip_credit_contract_check_card_button_num,
        last_1_year_drip_repayment_button_num,
        last_6_month_drip_repayment_button_num,
        last_6_month_drip_repayment_result_page_num,
        last_7_day_drip_repayment_result_page_num,
        his_drip_auth_click_num,

    -- feature_individual_manhattan_drip_overdue
        curren_drip_overdue_day_max,
        first_drip_overdue_0_total,
        his_drip_overdue_money_total,
        today_drip_overdue_money,
        datediff(to_date(label.server_date),to_date(last_drip_overdue_time)) as last_drip_overdue_time,
        last_30_day_drip_max_overdue_days,

    -- feature_individual_whole_passenger_base_info
        coin,
        is_beatles_driver, --是否顺风车主
        last_fast_finish_days,
        pas_age, --乘客年龄,-1代表未知
        first_call_days,
        first_gulf_call_days,

    -- feature_individual_omega_passenger_gaopao
        last_2_year_hit_loan_app_cnt,

    -- feature_individual_manhattan_drip_coupon
        datediff(to_date(label.server_date),to_date(first_taken_time)) as first_taken_time,
        last_7_day_drip_taken_type_2_coupon_num,
        last_6_month_drip_taken_coupon_num,
        last_1_year_drip_taken_coupon_num,
        his_drip_use_coupon_amt,
        last_1_year_drip_use_coupon_num,
        last_7_day_drip_use_coupon_num,
        his_drip_taken_coupon_num,
        datediff(to_date(label.server_date),to_date(last_use_time)) as last_use_time,

    -- feature_individual_omega_passenger_whole
        cell_phone_os,
        app_life_tools_category_number, --生活休闲-小工具类APP个数
        app_shop_mall_category_number,
        app_stock_sub_category_number_ratio ,
        app_shop_discount_category_number_ratio ,
        app_audio_category_number,
        app_audio_music_category_number_ratio ,
        app_financial_category_number_ratio ,

    -- feature_individual_recombination_passenger_city_age
        last_1_year_total_finish_order_actual_cost_rank,--城市和年龄段中最近1年完成专快出订单实付金额排序
        last_6_month_total_finish_order_actual_cost,--最近6月专快出总流水
        phone_model_price_rank,
        resident_night_dest_avg_unit_price_rank,
        resident_morning_start_avg_unit_price_rank,

    -- feature_individual_excavate_passenger_platform
    --     is_businessman,

    -- feature_individual_recombination_passenger_city_age_percentile_ratio
        last_6_month_total_finish_order_actual_cost_bining_ratio,
        phone_model_price_bining_ratio,
        last_1_year_total_finish_order_actual_cost_bining_ratio,
        phone_model_price_percentil,

    -- user_loan_demand_event_stat_fea_20200701_20200725
        ratio_6,
        last_1d_market_biopen_cnt,
        ratio_1,
        ratio_5,
        last_1m_market_biopen_cnt,
        ratio_4,
        ratio_2,
        last_1d_loan_confirm_button_ck_cnt,
        last_3d_market_biopen_cnt,
        last_1d_market_setup_sw_cnt,
        days_from_last_market_biopen,
        last_1d_input_pay_password_popup_cnt,
        days_from_last_market_loan_list_sw,
        days_from_last_market_input_pay_password_popup,
        last_1d_loan_apply_submit_api_done_cnt,
        days_from_last_market_loan_confirm_button_ck,
        last_1m_market_setup_sw_cnt,
        last_1d_market_loan_list_sw_cnt,
        days_from_last_market_setup_sw,
        days_from_last_loan_apply_submit_api_done,
        last_7d_input_pay_password_popup_cnt,
        last_3d_loan_apply_submit_api_done_cnt,
        last_7d_market_loan_list_sw_cnt,
        last_3d_loan_page_show_cnt,
        last_1d_face_rec_button_cnt,
        last_1m_market_loan_list_sw_cnt,
        days_from_last_market_loan_page_show,
        ratio_8,
        last_1d_loan_page_show_cnt,
        last_3d_market_loan_list_sw_cnt,
        last_7d_loan_page_show_cnt,
        last_7d_loan_apply_submit_api_done_cnt,
        ratio_3,
        last_7d_market_setup_sw_cnt,
        is_last_1d_market_biopen,
        last_7d_market_biopen_cnt,
        ratio_7,
        is_last_1d_loan_confirm_button_ck,
        last_1m_input_pay_password_popup_cnt

    from
    (

        select
            uid,
            server_date,
            date_sub(date_sub(server_date,1),pmod(datediff(date_sub(server_date,1),'2012-01-01'),7)) as last_week_align
        from manhattan_test.dsd_user_loan_demand_samples_label_${start_pt}_${end_pt}
        where funds_channel_id is not NULL
        group by uid,server_date
    ) label

    left join
    (
        select
            uid,
            last_credit_success_end_time,
            his_drip_max_credit_line,
            first_drip_apply_loan_pass_time,
            last_credit_success_start_time,
            last_drip_apply_loan_time,
            current_drip_interest_rate,
            last_drip_funds_channel_id,
            last_drip_apply_loan_pass_time,
            last_credit_identity_card_city_id,
            his_drip_min_credit_line,
            last_credit_mobile_city_id,
            last_bind_card_time,
            last_credit_identity_card_user_age,
            total_drip_apply_loan_suc_funds_channel_num,
            current_drip_credit_line,
            concat_ws('-',year,month,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_credit
        where concat_ws('-',year,month,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_credit
    on label.uid = feature_individual_manhattan_drip_credit.uid
    and date_sub(label.server_date,1) = feature_individual_manhattan_drip_credit.pt

    left join
    (
        select
            uid,
            last_drip_plan_end_time,
            last_drip_real_repay_time,
            current_drip_plan_end_time,
            last_30_day_drip_repay_money,
            last_drip_before_real_repay_time,
            last_30_day_drip_repay_principal,
            last_3_month_drip_repay_principal,
            last_7_day_drip_repay_principal,
            last_3_month_drip_repay_money,
            min_drip_mon_paid_principal,
            last_1_year_drip_before_repay_num,
            last_14_day_drip_repay_num,
            last_6_month_drip_repay_num,
            last_7_day_drip_repay_money,
            his_drip_single_three_days_num,
            last_14_day_drip_repay_money,
            last_14_day_drip_repay_principal,
            last_7_day_drip_before_repay_num,
            current_drip_real_end_time,
            his_drip_repay_num,
            last_30_day_drip_weekend_repay_money,
            last_1_year_drip_repay_num,
            last_6_month_drip_before_repay_num,
            last_6_month_drip_single_three_days_num,
            current_drip_account_balanced_days,
            last_30_day_drip_before_paid_principal,
            last_3_month_drip_weekend_repay_money,
            last_7_day_drip_weekend_repay_money,
            concat_ws('-',year,month,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_repay
        where concat_ws('-',year ,month,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_repay
    on label.uid = feature_individual_manhattan_drip_repay.uid
    and date_sub(label.server_date,1) = feature_individual_manhattan_drip_repay.pt

    left join
    (
        select
            uid,
            last_drip_disburse_ucc_time,
            last_drip_disburse_failed_time,
            his_drip_repay_not_done_num,
            avg_drip_total_money,
            current_drip_in_loan_money,
            last_7_day_drip_loan_failed_num,
            last_3_month_drip_loan_failed_num,
            last_30_day_drip_max_loan_day_principal,
            last_drip_isburse_succ_principal,
            last_14_day_drip_loan_failed_num,
            second_drip_disburse_succ_time,
            last_7_day_drip_max_loan_day_principal,
            last_6_month_drip_loan_num,
            last_3_month_drip_loan_failed_money,
            last_3_month_drip_loan_num,
            first_drip_disburse_succ_principal,
            last_1_year_drip_loan_failed_money,
            avg_drip_mon_loan_money,
            his_drip_repay_done_num,
            last_6_month_drip_loan_failed_money,
            first_drip_disburse_failed_time,
            last_30_day_drip_loan_failed_num,
            last_1_year_drip_loan_failed_num,
            last_3_month_drip_max_loan_day_principal,
            last_14_day_drip_max_loan_day_principal,
            first_disburse,
            last_6_month_drip_loan_failed_num,
            last_1_year_drip_night_loan_num,
            last_7_day_drip_loan_failed_money,
            his_drip_loan_money,
            last_30_day_drip_loan_money,
            first_drip_disburse_succ_duration,
            last_1_year_drip_loan_num,
            last_6_month_drip_loan_money,
            last_30_day_drip_loan_failed_money,
            min_drip_total_duration,
            last_14_day_drip_loan_money,
            last_3_month_drip_loan_money,
            first_drip_disburse_succ_time,
            last_14_day_drip_loan_failed_money,
            last_6_month_drip_max_loan_day_principal,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_disburse
        where concat_ws('-',year ,month ,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_disburse
    on label.uid = feature_individual_manhattan_drip_disburse.uid
    and date_sub(label.server_date,1) = feature_individual_manhattan_drip_disburse.pt

    left join
    (
        select
            uid,
            last_3_month_drip_num,
            his_face_num,
            last_6_month_drip_num,
            last_7_day_drip_num,
            last_1_year_drip_num,
            his_drip_num,
            last_face_submit_time,
            his_drip_homepage_num,
            last_1_year_drip_loan_result_page_num,
            his_drip_loan_result_page_num,
            last_7_day_drip_loan_result_page_num,
            last_30_day_drip_num,
            last_6_month_drip_loan_result_page_num,
            last_30_day_drip_loan_result_page_num,
            last_3_month_drip_loan_result_page_num,
            his_drip_repayment_result_page_num,
            last_14_day_drip_repayment_result_page_num,
            last_14_day_drip_num,
            last_30_day_drip_repayment_button_num,
            last_14_day_drip_loan_result_page_num,
            last_14_day_drip_total_repayment_button_num,
            last_7_day_drip_total_repayment_button_num,
            last_3_month_drip_repayment_button_num,
            last_1_year_drip_total_repayment_button_num,
            his_drip_credit_contract_check_card_button_num,
            last_30_day_drip_repayment_result_page_num,
            last_3_month_drip_repayment_result_page_num,
            last_1_year_drip_credit_contract_check_card_button_num,
            last_1_year_drip_repayment_button_num,
            last_6_month_drip_repayment_button_num,
            last_6_month_drip_repayment_result_page_num,
            last_7_day_drip_repayment_result_page_num,
            his_drip_auth_click_num,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_flow
        where concat_ws('-',year ,month ,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_flow
    on label.uid = feature_individual_manhattan_drip_flow.uid
    and date_sub(label.server_date,1) = feature_individual_manhattan_drip_flow.pt

    left join
    (
        select
            uid,
            curren_drip_overdue_day_max,
            first_drip_overdue_0_total,
            his_drip_overdue_money_total,
            today_drip_overdue_money,
            last_drip_overdue_time,
            last_30_day_drip_max_overdue_days,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_overdue
        where concat_ws('-',year ,month ,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_overdue
    on label.uid = feature_individual_manhattan_drip_overdue.uid
    and date_sub(label.server_date,1) = feature_individual_manhattan_drip_overdue.pt

    left join
    (
        select
            uid,
            coin,
            case when is_beatles_driver = 1  then 1 else 0 end as is_beatles_driver, --是否顺风车主
            datediff(concat_ws('-',year,month,day), last_fast_finish_time) last_fast_finish_days,
            case when pas_age > 0 then pas_age else NULL end as pas_age, --乘客年龄,-1代表未知
            datediff(concat_ws('-',year,month,day), first_call_time) first_call_days,
            datediff(concat_ws('-',year,month,day), first_gulf_call_time) first_gulf_call_days,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_whole_passenger_base_info
        where concat_ws('-',year ,month ,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_whole_passenger_base_info
    on label.uid = feature_individual_whole_passenger_base_info.uid
    and date_sub(label.server_date,1) = feature_individual_whole_passenger_base_info.pt

    left join
    (
        select
            uid,
            last_2_year_hit_loan_app_cnt,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_omega_passenger_gaopao
        where concat_ws('-',year,month,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_omega_passenger_gaopao
    on label.uid = feature_individual_omega_passenger_gaopao.uid
    and date_sub(label.server_date,1) = feature_individual_omega_passenger_gaopao.pt

    left join
    (
        select
            uid,
            first_taken_time,
            last_7_day_drip_taken_type_2_coupon_num,
            last_6_month_drip_taken_coupon_num,
            last_1_year_drip_taken_coupon_num,
            his_drip_use_coupon_amt,
            last_1_year_drip_use_coupon_num,
            last_7_day_drip_use_coupon_num,
            his_drip_taken_coupon_num,
            last_use_time,
            concat_ws('-',year ,month ,day) as pt
        from riskmanage_dm.feature_individual_manhattan_drip_coupon
        where concat_ws('-',year,month,day) between date_sub('${start_date}',1) and '${end_date}'
    )feature_individual_manhattan_drip_coupon
    on label.uid =feature_individual_manhattan_drip_coupon.uid
    and date_sub(label.server_date,1)=feature_individual_manhattan_drip_coupon.pt

    left join
    (
        select
            uid,
            case when upper(split(last_30_days_model_top_list, '!#')[0]) rlike 'IPHONE' then 1 else 0 end as cell_phone_os,
            app_life_tools_category_number, --生活休闲-小工具类APP个数
            app_shop_mall_category_number,
            app_stock_sub_category_number / app_financial_category_number as   app_stock_sub_category_number_ratio ,
            app_shop_discount_category_number/app_shop_category_number as   app_shop_discount_category_number_ratio ,
            app_audio_category_number,
            app_audio_music_category_number / app_audio_category_number as   app_audio_music_category_number_ratio ,
            app_financial_category_number / app_total_number as app_financial_category_number_ratio ,
            concat_ws('-', year, month, day) as pt
        from
            riskmanage_dm.feature_individual_omega_passenger_whole
        where concat_ws('-',year,month,day) between date_sub('${start_date}',1) and '${end_date}'
    ) feature_individual_omega_passenger_whole
    on label.uid = feature_individual_omega_passenger_whole.uid
    and date_sub(label.server_date,1) = feature_individual_omega_passenger_whole.pt

    left join
    (
        select
            uid,
            last_1_year_total_finish_order_actual_cost_rank,--城市和年龄段中最近1年完成专快出订单实付金额排序
            last_6_month_total_finish_order_actual_cost,--最近6月专快出总流水
            phone_model_price_rank,
            resident_night_dest_avg_unit_price_rank,
            resident_morning_start_avg_unit_price_rank,
            concat_ws('-',year,month,day) as pt
        from
            riskmanage_dm.feature_individual_recombination_passenger_city_age
        where
            concat_ws('-',year,month,day) between date_sub('${start_date}',7) and '${end_date}'
            and date_format(concat_ws('-', year, month, day), 'u')=7
    ) feature_individual_recombination_passenger_city_age
    on label.uid = feature_individual_recombination_passenger_city_age.uid
    and label.last_week_align = feature_individual_recombination_passenger_city_age.pt

    left join
    (
        select
            uid,
            last_6_month_total_finish_order_actual_cost_bining_ratio,
            phone_model_price_bining_ratio,
            last_1_year_total_finish_order_actual_cost_bining_ratio,
            phone_model_price_percentil,
            concat_ws('-',year,month,day) as pt
        from
            riskmanage_dm.feature_individual_recombination_passenger_city_age_percentile_ratio
        where
            concat_ws('-',year,month,day) between date_sub('${start_date}',7) and '${end_date}'
            and date_format(concat_ws('-', year, month, day), 'u')=7
    ) feature_individual_recombination_passenger_city_age_percentile_ratio
    on label.uid = feature_individual_recombination_passenger_city_age_percentile_ratio.uid
    and label.last_week_align = feature_individual_recombination_passenger_city_age_percentile_ratio.pt

    left join

    (
            select
                t0.uid,
                t0.stat_date,
                current_drip_loan_credit_line_available,
                current_drip_loan_credit_line_used / current_drip_loan_credit_line as current_drip_loan_credit_line_use_rate
            from
                (
                    select
                        uid, funds_channel_id, cfrnid, stat_date
                    from
                        (
                            select
                                uid,
                                funds_channel_id,
                                cfrnid,
                                CONCAT_WS('-', year, month, day) as stat_date,
                                row_number() over(partition by uid, CONCAT_WS('-', year, month, day) order by success_time desc) as rk
                            from
                                manhattan_dw.dwd_loan_user_credit_d_whole
                            where
                                concat_ws('-',year,month,day)  between date_add('${start_date}',-1) and '${end_date}'
                                and success_time like '20%'
                                and product_id=2001
                                and status = 1
                        ) t0
                    where
                        rk = 1
                ) t0

                left outer join
                (
                    select
                        cfrnid,
                        funds_channel_id,
                        fixed_limit_used /100 as current_drip_loan_credit_line_used ,
                        fixed_limit_available /100 as current_drip_loan_credit_line_available,
                        fixed_limit/100 as current_drip_loan_credit_line,
                        CONCAT_WS('-', year, month, day) as stat_date
                    from
                        manhattan_dw.dwd_loan_credit_limit_d_whole
                    where
                        concat_ws('-',year,month,day) between date_add('${start_date}',-1) and '${end_date}'
                        and product_id=2001
                        and fixed_limit_status =0
                        and nvl(di_status, 0)=0
                ) t1
                on t0.cfrnid=t1.cfrnid and t0.funds_channel_id = t1.funds_channel_id and t0.stat_date = t1.stat_date
    )current_drip_credit
    on label.uid = current_drip_credit.uid
    and date_sub(label.server_date,1) = current_drip_credit.stat_date

    left join
    (
        select
            uid,
            server_date,
            ratio_6,
            last_1d_market_biopen_cnt,
            ratio_1,
            ratio_5,
            last_1m_market_biopen_cnt,
            ratio_4,
            ratio_2,
            last_1d_loan_confirm_button_ck_cnt,
            last_3d_market_biopen_cnt,
            last_1d_market_setup_sw_cnt,
            days_from_last_market_biopen,
            last_1d_input_pay_password_popup_cnt,
            days_from_last_market_loan_list_sw,
            days_from_last_market_input_pay_password_popup,
            last_1d_loan_apply_submit_api_done_cnt,
            days_from_last_market_loan_confirm_button_ck,
            last_1m_market_setup_sw_cnt,
            last_1d_market_loan_list_sw_cnt,
            days_from_last_market_setup_sw,
            days_from_last_loan_apply_submit_api_done,
            last_7d_input_pay_password_popup_cnt,
            last_3d_loan_apply_submit_api_done_cnt,
            last_7d_market_loan_list_sw_cnt,
            last_3d_loan_page_show_cnt,
            last_1d_face_rec_button_cnt,
            last_1m_market_loan_list_sw_cnt,
            days_from_last_market_loan_page_show,
            ratio_8,
            last_1d_loan_page_show_cnt,
            last_3d_market_loan_list_sw_cnt,
            last_7d_loan_page_show_cnt,
            last_7d_loan_apply_submit_api_done_cnt,
            ratio_3,
            last_7d_market_setup_sw_cnt,
            is_last_1d_market_biopen,
            last_7d_market_biopen_cnt,
            ratio_7,
            is_last_1d_loan_confirm_button_ck,
            last_1m_input_pay_password_popup_cnt
        from manhattan_test.user_demand_model_event_stat_fea_${start_pt}_${end_pt}
    )user_demand_model_event_stat_fea_${start_pt}_${end_pt}
    on label.uid = user_demand_model_event_stat_fea_${start_pt}_${end_pt}.uid
    and label.server_date = user_demand_model_event_stat_fea_${start_pt}_${end_pt}.server_date
) feat

left join

(
    select
        uid,
        server_date,
        label
    from manhattan_test.dsd_user_loan_demand_samples_label_${start_pt}_${end_pt}
    where funds_channel_id is not NULL
)sample_label
on feat.uid = sample_label.uid
and feat.server_date = sample_label.server_date
;"
