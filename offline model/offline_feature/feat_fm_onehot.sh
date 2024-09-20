#!/bin/bash
source ~/.bash_profile


hive -e "

create table manhattan_test.dsd_user_loan_demand_FM_feature as 
select
    account.uid   ,
    account.funds_channel_id      ,
    account.tel   ,
    account.label ,
    account.server_date   ,
    --额度等级--
    credit_line_level_1   ,
    credit_line_level_2   ,
    credit_line_level_3   ,
    credit_line_level_4   ,
    credit_line_level_5   ,
    credit_line_level_6   ,
    --利率等级--
    current_drip_interest_rate_level_1    ,
    current_drip_interest_rate_level_2    ,
    current_drip_interest_rate_level_3    ,
    current_drip_interest_rate_level_4    ,
    current_drip_interest_rate_level_5    ,
    current_drip_interest_rate_level_6    ,
    current_drip_interest_rate_level_7    ,
    current_drip_interest_rate_level_8    ,
    --剩余额度等级--
    current_drip_credit_line_balance_level_1      ,
    current_drip_credit_line_balance_level_2      ,
    current_drip_credit_line_balance_level_3      ,
    current_drip_credit_line_balance_level_4      ,
    current_drip_credit_line_balance_level_5      ,
    current_drip_credit_line_balance_level_6      ,
    --在贷金额等级--
    plan_amount_level_1   ,
    plan_amount_level_2   ,
    plan_amount_level_3   ,
    plan_amount_level_4   ,
    plan_amount_level_5   ,
    plan_amount_level_6   ,
    plan_amount_level_7   ,
    plan_amount_level_8   ,
    plan_amount_level_9   ,
    --在贷期数等级--
    duration_num_level_1  ,
    duration_num_level_2  ,
    duration_num_level_3  ,
    duration_num_level_4  ,
    duration_num_level_5  ,
    duration_num_level_6  ,
    --优惠券等级--
    current_valid_coupon_num_level_1      ,
    current_valid_coupon_num_level_2      ,
    current_valid_coupon_num_level_3      ,
    current_valid_coupon_num_level_4      ,

    --账龄等级--
    last_credit_success_start_time_level_1,
    last_credit_success_start_time_level_2,
    last_credit_success_start_time_level_3,
    last_credit_success_start_time_level_4,
    last_credit_success_start_time_level_5,
    last_credit_success_end_time_level_6  ,

    fin_age_level_1,
    fin_age_level_2,
    fin_age_level_3,
    fin_age_level_4,
    fin_age_level_5,
    fin_age_level_6,
    fin_age_level_7,
    fin_age_level_8,

    is_beatles_driver,

    coin_level_1,
    coin_level_2,
    coin_level_3,
    coin_level_4,
    coin_level_5,
    coin_level_6,
    coin_level_7,
    coin_level_8,
    
    --出行时长--
    first_call_days_level_1,
    first_call_days_level_2,
    first_call_days_level_3,
    first_call_days_level_4,
    first_call_days_level_5,
    first_call_days_level_6,
    first_call_days_level_7,
    
    --app--
    hit_loan_app_cnt_level_1,
    hit_loan_app_cnt_level_2,
    hit_loan_app_cnt_level_3,
    hit_loan_app_cnt_level_4,
    hit_loan_app_cnt_level_5,
    
    --资产等级--
    price_rank_level_1,
    price_rank_level_2,
    price_rank_level_3,
    price_rank_level_4,
    price_rank_level_5,

    --点击率--
    ratio_6_level_1,
    ratio_6_level_2,
    ratio_6_level_3,
    ratio_6_level_4,
    ratio_6_level_5,
    --支用活跃度等级--
    last_6_month_drip_loan_num_level_1,
    last_6_month_drip_loan_num_level_2,
    last_6_month_drip_loan_num_level_3,
    last_6_month_drip_loan_num_level_4,
    last_6_month_drip_loan_num_level_5,

    --支用金额等级--
    last_6_month_drip_loan_money_level_1,
    last_6_month_drip_loan_money_level_2,
    last_6_month_drip_loan_money_level_3,
    last_6_month_drip_loan_money_level_4,
    last_6_month_drip_loan_money_level_5,
    last_6_month_drip_loan_money_level_6,

    pas_sex ,
    is_fast_or_gulf_driver,
    company_occupation_1,
    company_occupation_2,
    company_occupation_3,
    company_occupation_4,
    company_occupation_5,
    company_occupation_6,
    company_occupation_7,
    company_occupation_8,
    company_occupation_9,
    is_businessman


from
(
    select 
        uid   ,
        funds_channel_id      ,
        tel   ,
        label ,
        server_date   ,
        date_sub(date_sub(server_date,1),pmod(datediff(date_sub(server_date,1),'2012-01-01'),7)) as last_week_align,
        --额度等级--
        credit_line_level_1   ,
        credit_line_level_2   ,
        credit_line_level_3   ,
        credit_line_level_4   ,
        credit_line_level_5   ,
        credit_line_level_6   ,
        --利率等级--
        current_drip_interest_rate_level_1    ,
        current_drip_interest_rate_level_2    ,
        current_drip_interest_rate_level_3    ,
        current_drip_interest_rate_level_4    ,
        current_drip_interest_rate_level_5    ,
        current_drip_interest_rate_level_6    ,
        current_drip_interest_rate_level_7    ,
        current_drip_interest_rate_level_8    ,
        --剩余额度等级--
        current_drip_credit_line_balance_level_1      ,
        current_drip_credit_line_balance_level_2      ,
        current_drip_credit_line_balance_level_3      ,
        current_drip_credit_line_balance_level_4      ,
        current_drip_credit_line_balance_level_5      ,
        current_drip_credit_line_balance_level_6      ,
        --在贷金额等级--
        plan_amount_level_1   ,
        plan_amount_level_2   ,
        plan_amount_level_3   ,
        plan_amount_level_4   ,
        plan_amount_level_5   ,
        plan_amount_level_6   ,
        plan_amount_level_7   ,
        plan_amount_level_8   ,
        plan_amount_level_9   ,
        --在贷期数等级--
        duration_num_level_1  ,
        duration_num_level_2  ,
        duration_num_level_3  ,
        duration_num_level_4  ,
        duration_num_level_5  ,
        duration_num_level_6  ,
        --优惠券等级--
        current_valid_coupon_num_level_1      ,
        current_valid_coupon_num_level_2      ,
        current_valid_coupon_num_level_3      ,
        current_valid_coupon_num_level_4      ,

        --账龄等级--
        last_credit_success_start_time_level_1,
        last_credit_success_start_time_level_2,
        last_credit_success_start_time_level_3,
        last_credit_success_start_time_level_4,
        last_credit_success_start_time_level_5,
        last_credit_success_end_time_level_6  

    from manhattan_test.user_demand_model_account_user_fea_20200710_20200810
) account

left join

(
    select 
        uid,
        server_date,

        --年龄-积分-身份-
         
        case when pas_age >=18 and pas_age <=22 then 1 else 0 end as fin_age_level_1,
        case when pas_age >22 and pas_age <= 26 then 1 else 0 end as fin_age_level_2,
        case when pas_age >26 and pas_age <= 30 then 1 else 0 end as fin_age_level_3,
        case when pas_age >30 and pas_age <= 35 then 1 else 0 end as fin_age_level_4,
        case when pas_age >35 and pas_age <= 40 then 1 else 0 end as fin_age_level_5,
        case when pas_age >40 and pas_age <= 45 then 1 else 0 end as fin_age_level_6,
        case when pas_age >45 and pas_age <= 55 then 1 else 0 end as fin_age_level_7,
        case when pas_age >55 then 1 else 0 end as fin_age_level_8,

        is_beatles_driver,

        case when coin <=0 then 1 else 0 end as coin_level_1,
        case when coin >0 and coin <= 250 then 1 else 0 end as coin_level_2,
        case when coin >250 and coin <= 500 then 1 else 0 end as coin_level_3,
        case when coin >500 and coin <= 750 then 1 else 0 end as coin_level_4,
        case when coin >750 and coin <= 1000 then 1 else 0 end as coin_level_5,
        case when coin >1000 and coin <= 5000 then 1 else 0 end as coin_level_6,
        case when coin >5000 and coin <= 10000 then 1 else 0 end as coin_level_7,
        case when coin >10000 then 1 else 0 end as coin_level_8,
        
        --出行时长--
        case when first_call_days <= 365 then 1 else 0 end as first_call_days_level_1,
        case when first_call_days > 365 and first_call_days <= 730 then 1 else 0 end as first_call_days_level_2,
        case when first_call_days > 730 and first_call_days <= 1095 then 1 else 0 end as first_call_days_level_3,
        case when first_call_days > 1095 and first_call_days <= 1460 then 1 else 0 end as first_call_days_level_4,
        case when first_call_days > 1460 and first_call_days <= 1825 then 1 else 0 end as first_call_days_level_5,
        case when first_call_days > 1825 and first_call_days <= 2190 then 1 else 0 end as first_call_days_level_6,
        case when first_call_days > 2190 then 1 else 0 end as first_call_days_level_7,
        
        --app--
        case when last_2_year_hit_loan_app_cnt <= 3 then 1 else 0 end as hit_loan_app_cnt_level_1,
        case when last_2_year_hit_loan_app_cnt > 3 and last_2_year_hit_loan_app_cnt <= 6 then 1 else 0 end as  hit_loan_app_cnt_level_2,
        case when last_2_year_hit_loan_app_cnt > 6 and last_2_year_hit_loan_app_cnt <= 10 then 1 else 0 end as  hit_loan_app_cnt_level_3,
        case when last_2_year_hit_loan_app_cnt > 10 and last_2_year_hit_loan_app_cnt <= 20 then 1 else 0 end as  hit_loan_app_cnt_level_4,
        case when last_2_year_hit_loan_app_cnt > 20  then 1 else 0 end as  hit_loan_app_cnt_level_5,
        
        --资产等级--
        case when last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank < 10000 then 1 else 0 end as price_rank_level_1,
        case when last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank >= 10000 
        and last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank <= 100000
        then 1 else 0 end as price_rank_level_2,
        
        case when last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank >= 100000 
        and last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank <= 200000
        then 1 else 0 end as price_rank_level_3,

        case when last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank >= 200000 
        and last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank <= 300000
        then 1 else 0 end as price_rank_level_4,

        case when last_1_year_total_finish_order_actual_cost_rank + phone_model_price_rank + resident_night_dest_avg_unit_price_rank >= 300000 
        then 1 else 0 end as price_rank_level_5,


        --点击转化率--
        case when ratio_6 < 0.2 then 1 else 0 end as ratio_6_level_1,
        case when ratio_6 >= 0.2 and ratio_6 < 0.4 then 1 else 0 end as ratio_6_level_2,
        case when ratio_6 >= 0.4 and ratio_6 < 0.6 then 1 else 0 end as ratio_6_level_3,
        case when ratio_6 >= 0.6 and ratio_6 < 0.8 then 1 else 0 end as ratio_6_level_4,
        case when ratio_6 >= 0.8 and ratio_6 <= 1 then 1 else 0 end as ratio_6_level_5,
        --支用活跃度等级--
        last_6_month_drip_loan_num,
        case when last_6_month_drip_loan_num < 5 then 1 else 0 end as last_6_month_drip_loan_num_level_1,
        case when last_6_month_drip_loan_num >= 5 and last_6_month_drip_loan_num < 10 then 1 else 0 end as last_6_month_drip_loan_num_level_2,
        case when last_6_month_drip_loan_num >= 10 and last_6_month_drip_loan_num < 20 then 1 else 0 end as last_6_month_drip_loan_num_level_3,
        case when last_6_month_drip_loan_num >= 20 and last_6_month_drip_loan_num < 50 then 1 else 0 end as last_6_month_drip_loan_num_level_4,
        case when last_6_month_drip_loan_num >= 50 then 1 else 0 end as last_6_month_drip_loan_num_level_5,

        --支用金额等级--
        case when last_6_month_drip_loan_money < 5000 then 1 else 0 end as last_6_month_drip_loan_money_level_1,
        case when last_6_month_drip_loan_money >= 5000 and last_6_month_drip_loan_money < 10000 then 1 else 0 end as last_6_month_drip_loan_money_level_2,
        case when last_6_month_drip_loan_money >= 10000 and last_6_month_drip_loan_money < 50000 then 1 else 0 end as last_6_month_drip_loan_money_level_3,
        case when last_6_month_drip_loan_money >= 50000 and last_6_month_drip_loan_money < 100000 then 1 else 0 end as last_6_month_drip_loan_money_level_4,
        case when last_6_month_drip_loan_money >= 100000 and last_6_month_drip_loan_money < 200000 then 1 else 0 end as last_6_month_drip_loan_money_level_5,
        case when last_6_month_drip_loan_money >= 200000 then 1 else 0 end as last_6_month_drip_loan_money_level_6


    from manhattan_test.dsd_user_loan_demand_xgb_stat_20200710_20200810
)stat
on account.uid = stat.uid and account.server_date = stat.server_date

left join
(
    select 
        uid,
        concat_ws('-',year,month,day) as pt,
        case when pas_sex < 0 then NULL else pas_sex end as pas_sex ,--乘客性别,-9999代表未知
        COALESCE(is_fast_or_gulf_driver_by_card,is_fast_or_gulf_driver) as is_fast_or_gulf_driver
    from riskmanage_dm.feature_individual_whole_passenger_base_info
        where concat_ws('-',year,month,day) between '2020-06-30' and '2020-08-10'
) feature_individual_whole_passenger_base_info
on account.uid=feature_individual_whole_passenger_base_info.uid
and date_sub(account.server_date,1) = feature_individual_whole_passenger_base_info.pt

left join
(
    select
        uid,
        concat_ws('-',year,month,day) as pt,
        --1.机关人员 2.教育工作者 3.医学美容类从业人员 4.金融业 5.公职服务人员 6.白领 7.服务业从业人员 8.个体户 9.蓝领 10.其他
        case when company rlike '.*(分局|支局|政府|管理局|大厅|地税局|派出所|管理所|管理中心|支队|管理处|教育局|气象局|法院|公路局|检察院|保障局|林业局|管理站|设计院|电视台|信访局|地税所).*' or company rlike '.*局$' then 1 else 0 end as company_occupation_1,
        case when company rlike '.*(小学|中学|学校|学院|校区|分校|技术学校|培训中心|研究所|研究院|党校|教学楼|中心校|大学|职业中专).*' then 1 else 0 end as company_occupation_2,
        case when company rlike '.*(医院|药房|门诊|门诊部|体检中心|保健院).*' then 1 else 0 end as company_occupation_3,
        case when company rlike '.*(银行|营业厅|邮政储蓄|邮政|信用社|人行|营业点).*' then 1 else 0 end as company_occupation_4,
        case when company rlike '.*(委员会|社区|街道|服务中心|服务站|社区卫生|办事处|村委|警务室|街道办|大队|工作站|客运站|收费站|车站|地铁站|服务区|检测站|东站|生态园|景区|协会|馆|报名处|体育场|训练场|公园|旅游区|名胜区|机场).*' then 1 else 0 end as company_occupation_5,
        case when company rlike '.*(公司|中心|集团|大厦|工业园|开发区|基地|产业|工业区|大楼|科技园|园区|创业园|写字楼|技术开发区|商务楼|软件园|孵化园|创意园|创意工场|SOHO).*' then 1 else 0 end as company_occupation_6,
        case when company rlike '.*(店|广场|宾馆|酒店|驾校|百货|购物广场|营销中心|购物中心|商场|餐厅|营业部|网吧|俱乐部|商贸城|生活区|旅馆|酒楼|客栈|售楼处|乐园|生活馆|影城|售楼部|银座|会展中心|百货大楼|商业城|影剧院).*' then 1 else 0 end as company_occupation_7,
        case when company rlike '.*(市场|农场|批发部|建材城|家具城|汽车城|林场|菜场|服装城).*' then 1 else 0 end as company_occupation_8,
        case when company rlike '.*(物流园|厂|工地)$' then 1 else 0 end as company_occupation_9,
        
        is_businessman  -- decimal(19,6)   商旅人士概率

    from
        riskmanage_dm.feature_individual_excavate_passenger_platform
    where
        concat_ws('-',year,month,day) between '2020-06-23' and '2020-08-10'
        and date_format(concat_ws('-', year, month, day), 'u')=7
) feature_individual_excavate_passenger_platform
on account.uid = feature_individual_excavate_passenger_platform.uid
and account.last_week_align = feature_individual_excavate_passenger_platform.pt




"
