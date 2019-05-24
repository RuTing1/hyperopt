#!/bin/bash
#author: DR
#date: 2019-04-09
#desc: this script is used to extract features from crpl for antifraud lending model
#parameter: $1: calculate_day,特征计算的时间
#example nohup sh fraud_users_0315.sh 2018-04-28


calculate_day=$1

#建立数据样本表，分区表示训练集和测试集选取时间截点
hive -S -e "q
USE ft_tmp;
CREATE TABLE IF NOT EXISTS dwd_cprl_antifraud_trans_feas(
jdpin    string     COMMENT    'jdpin',
max_monthly_income    double    COMMENT    '月收入最大值',
min_monthly_income    double    COMMENT    '月收入最小值',
highest_eduction    bigint    COMMENT    '最高教育程度',
accumulation_fund_auth     bigint    COMMENT    '是否公积金授权',
id_card_auth    bigint    COMMENT    '是否上传身份证照片',
operator_auth    bigint    COMMENT    '是否运营商授权',
lend_id_succ_cnt    bigint    COMMENT    '申请成功机构数',
apply_passed_cnt    bigint    COMMENT    '申请成功订单数',
overdue_days        bigint    COMMENT    '当前最长逾期时间',
lend_id_failed_cnt    bigint    COMMENT    '申请失败机构数',
apply_failed_cnt    bigint    COMMENT    '申请失败订单数',
loan_amount    double    COMMENT    '申请额度',
cash_amount    double    COMMENT    '提现额度',
overdue_principal    double    COMMENT    '逾期金额',
loan_amount_b    double    COMMENT    '最后一次申请前申请额度',
cash_amount_b    double    COMMENT    '最后一次申请前提现额度',
--overdue_principal_b    double    COMMENT    '最后一次申请前逾期金额',
first_loan_amount    double    COMMENT    '首次申请额度',
first_cash_amount    double    COMMENT    '首次提现额度',
first_overdue_principal    double    COMMENT    '首次逾期金额',
max_cash_apply_date_diff    bigint    COMMENT    '提现时差最大值',
min_cash_apply_date_diff    bigint    COMMENT    '提现时差最小值',
avg_cash_apply_date_diff    bigint    COMMENT    '提现时差均值',
--max_apply_date_diff    bigint    COMMENT    '申请时差最大值',
--min_apply_date_diff    bigint    COMMENT    '申请时差最小值',
--avg_apply_date_diff    bigint    COMMENT    '申请时差均值',
max_overdue_time_diff    bigint    COMMENT    '逾期时差最大值',
min_overdue_time_diff    bigint    COMMENT    '逾期时差最小值',
avg_overdue_time_diff    bigint    COMMENT    '逾期时差均值',
max_apply_cash_date_diff    bigint    COMMENT    '申请提现时差最大值',
min_apply_cash_date_diff    bigint    COMMENT    '申请提现时差最小值',
avg_apply_cash_date_diff    bigint    COMMENT    '申请提现时差均值',
first_cash_apply_date_diff    bigint    COMMENT    '首次申请提现时差',
first_overdue_cash_date_diff    bigint    COMMENT    '首次逾期提现时差',
first_cash_ratio    double    COMMENT    '首次提现率',
first_overdue_ratio    double    COMMENT    '首次逾期率',
first_apply_date    string     COMMENT    '首次申请时间',
latest_apply_date    string     COMMENT    '最近一次申请时间')
COMMENT '反欺诈交易特征表' PARTITIONED BY (dt STRING COMMENT '操作日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
"


#建立临时表，借钱欺诈业务标签制作一次处理表
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig AS
SELECT user_jrid jrid
      ,eduction
      ,monthly_income
      ,accumulation_fund_auth
      ,id_card_auth  -- 是否上传身份证照片 (-1,0,1,2)
      ,operator_auth  --是否运营商授权
      ,lend_id  --放贷机构ID
      ,apply_status
      --,loan_amount
      ,loan_amount
      ,cash_amount
      ,reduction_amount
      ,loan_users.apply_no
      ,cash_id
      ,apply_date
      ,cash_apply_date
      ,principal
      ,repayment_time
      ,overdue_days
    FROM
    (
    SELECT *
          FROM
          (
          SELECT user_jrid
                ,apply_no
                ,CASE WHEN eduction='DOCTOR' THEN 6
                      WHEN eduction='MASTER' THEN 5
                      WHEN eduction='UNDERGRADUATE' THEN 4
                      WHEN eduction='JUNIORCOLLEGE' THEN 3
                      WHEN eduction='HIGHSCHOOL' THEN 2
                      WHEN eduction='MIDDLESCHOOL' THEN 1
                      ELSE 0 END eduction
                ,CASE WHEN monthly_income='LEVEL_1' THEN 1
                      WHEN monthly_income='LEVEL_2' THEN 2
                      WHEN monthly_income='LEVEL_3' THEN 3
                      WHEN monthly_income='LEVEL_4' THEN 4
                      WHEN monthly_income='LEVEL_5' THEN 5
                      WHEN monthly_income='LEVEL_6' THEN 6
                      WHEN monthly_income='LEVEL_7' THEN 7
                      ELSE -1 END monthly_income
                ,accumulation_fund_auth  -- 是否公积金授权
                ,id_card_auth  -- 是否上传身份证照片 (-1,0,1,2)
                ,operator_auth  --是否运营商授权
                ,lend_id  --放贷机构ID
                ,apply_status
                ,apply_date
                ,row_number()over(partition by apply_no ORDER BY modified_date DESC) rn
              FROM dwd.dwd_wallet_crpl_loan_apply_i_d
              WHERE instr(loan_product_name,'测试')=0
          )a
          WHERE rn=1
    )loan_users
    LEFT JOIN
    (
    SELECT jrid
          ,apply_no
          ,cash_id
          ,MAX(loan_amount) loan_amount    --授信额度
          ,MAX(cash_amount) cash_amount    --提现金额
          ,MIN(cash_apply_date) cash_apply_date --提现完成时间
          ,MIN(reduction_amount) reduction_amount
          ,MAX(status) status
        FROM
        (
        SELECT jrid
              ,apply_no
              ,cash_id
              ,loan_amount  --授信额度
              ,cash_amount  --提现金额
              ,period_count
              ,cash_complete_date  --提现完成时间
              ,cash_apply_date
              ,reduction_amount
              ,CASE WHEN status in ('LOANED','COMPLETE') THEN 1 ELSE 0 END status
              ,row_number()over(partition by cash_id ORDER BY modified_date DESC) rn
            FROM dwd.dwd_wallet_crpl_cash_apply_i_d
            WHERE instr(product_name,'测试')=0
                  AND modified_date <='${calculate_day}'
                  --AND status in ('LOANED','COMPLETE')
        )a
        WHERE rn=1
        GROUP BY jrid,apply_no,cash_id
    ) cash_users ON loan_users.apply_no=cash_users.apply_no
    LEFT JOIN
    (
    SELECT *
        FROM
        (
        SELECT order_no
              ,repayment_index
              ,principal
              ,datediff(dt,repayment_time) overdue_days
              ,to_date(repayment_time) repayment_time
            FROM dwd.dwd_wallet_crpl_repayment_plan_s_d
            WHERE dt='${calculate_day}'   --取订单最新状态
            AND status != 'PAID'  --用户有逾期未还款
        ) a  ---每笔订单最新状态
       WHERE repayment_time<'${calculate_day}'  --剔除未开始还款的订单项
    )repayment_info ON cash_users.cash_id = repayment_info.order_no;"

#获取用户最近一次申请和首次申请时间
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas4;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas4 AS
SELECT jrid
      ,MIN(case when cash_id is not null then apply_date else null end ) first_apply_date
      ,MAX(apply_date) latest_apply_date
    FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig
    GROUP BY jrid;"

#获取用户注册时信息
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas1;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas1 AS
SELECT jrid
      ,MAX(monthly_income) max_monthly_income
      ,MIN(monthly_income) min_monthly_income
      ,MAX(eduction) highest_eduction
      ,MAX(accumulation_fund_auth) accumulation_fund_auth -- 是否公积金授权
      ,MAX(id_card_auth) id_card_auth
      ,MAX(operator_auth) operator_auth
      ,COUNT(DISTINCT CASE WHEN apply_status='PASS' THEN lend_id ELSE NULL END) lend_id_succ_cnt
      ,COUNT(DISTINCT CASE WHEN apply_status='PASS' THEN apply_no ELSE NULL END) apply_passed_cnt
      ,COUNT(DISTINCT CASE WHEN apply_status!='PASS' THEN lend_id ELSE NULL END) lend_id_failed_cnt
      ,COUNT(DISTINCT CASE WHEN apply_status!='PASS' THEN apply_no ELSE NULL END) apply_failed_cnt
      ,MAX(overdue_days) overdue_days
    FROM
      (
      SELECT jrid
            ,monthly_income
            ,eduction
            ,accumulation_fund_auth
            ,id_card_auth
            ,operator_auth
            ,lend_id
            ,apply_no
            ,overdue_days
            ,apply_status
          FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig
      )a
    GROUP BY jrid;
"

#获取用户最近一次贷款前额度、提现、逾期情况
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas20;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas20 AS
SELECT jrid
      ,CASE WHEN cash_ratio>1 THEN loan_amount*cash_ratio ELSE loan_amount END loan_amount_b
      ,loan_amount cash_amount_b
      ,overdue_principal overdue_principal_b
    FROM
        (
        SELECT jrid
              ,SUM(loan_amount) loan_amount
              ,SUM(cash_amount) cash_amount
              ,SUM(overdue_principal) overdue_principal
              ,ceil(SUM(cash_amount)/SUM(loan_amount)) cash_ratio
            FROM
            (
            SELECT jrid
                  ,apply_no
                  ,MAX(loan_amount) loan_amount
                  ,SUM(cash_amount) cash_amount
                  ,SUM(overdue_principal) overdue_principal
                FROM
                (
                SELECT jrid
                      ,apply_no
                      ,cash_id
                      ,MAX(loan_amount) loan_amount
                      ,MAX(cash_amount) cash_amount
                      ,SUM(principal) overdue_principal
                    FROM
                    (
                    SELECT a.jrid
                          ,apply_no
                          ,cash_id
                          ,loan_amount
                          ,cash_amount
                          ,principal
                        FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                        LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                        WHERE apply_date<latest_apply_date
                    )a
                    GROUP BY jrid,apply_no,cash_id
                )a
                GROUP BY jrid,apply_no
            )a
            GROUP BY jrid
        ) a;
"

#聚类模型入模特征：获取用户当前贷款额度、提现、逾期情况
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas2;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas2 AS
SELECT jrid
      ,CASE WHEN cash_ratio>1 THEN loan_amount*cash_ratio ELSE loan_amount END loan_amount
      ,cash_amount
      ,overdue_principal
    FROM
        (
        SELECT jrid
              ,SUM(loan_amount) loan_amount
              ,SUM(cash_amount) cash_amount
              ,SUM(overdue_principal) overdue_principal
              ,ceil(SUM(cash_amount)/SUM(loan_amount)) cash_ratio
            FROM
            (
            SELECT jrid
                  ,apply_no
                  ,MAX(loan_amount) loan_amount
                  ,SUM(cash_amount) cash_amount
                  ,SUM(overdue_principal) overdue_principal
                FROM
                (
                SELECT jrid
                      ,apply_no
                      ,cash_id
                      ,MAX(loan_amount) loan_amount
                      ,MAX(cash_amount) cash_amount
                      ,SUM(principal) overdue_principal
                    FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig
                    WHERE cash_id IS NOT NULL
                    GROUP BY jrid,apply_no,cash_id
                )a
                GROUP BY jrid,apply_no
            )a
            GROUP BY jrid
        ) a;
"

#获取用户最近一笔贷款前借钱相关行为特征
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas3;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas3 AS
SELECT jrid
      ,MAX(cash_apply_date_diff) max_cash_apply_date_diff
      ,MIN(cash_apply_date_diff) min_cash_apply_date_diff
      ,AVG(cash_apply_date_diff) avg_cash_apply_date_diff
      ,MIN(cash_apply_date) first_cash_apply_date
      ,MAX(cash_apply_date) latest_cash_apply_date
      ,MAX(first_loan_amount) first_loan_amount
      ,SUM(first_cash_amount) first_cash_amount
      ,MAX(first_overdue_principal)first_overdue_principal
    FROM
    (
    SELECT a.jrid
          ,datediff(b.cash_apply_date,a.cash_apply_date) cash_apply_date_diff
          ,CASE WHEN a.rn=1 THEN loan_amount ELSE NULL END first_loan_amount
          ,CASE WHEN a.rn=1 THEN cash_amount ELSE NULL END first_cash_amount
          ,CASE WHEN a.rn=1 THEN overdue_principal ELSE NULL END first_overdue_principal
          ,a.cash_apply_date
        FROM
        (
        SELECT jrid
              ,cash_apply_date
              ,loan_amount
              ,cash_amount
              ,overdue_principal
              ,row_number()over(partition by jrid ORDER BY cash_apply_date) rn
            FROM
            (
            SELECT jrid
                  ,cash_id
                  ,MAX(loan_amount) loan_amount
                  ,MAX(cash_amount) cash_amount
                  ,SUM(principal) overdue_principal
                  ,MIN(cash_apply_date) cash_apply_date
                FROM
                (
                SELECT a.jrid
                      ,cash_id
                      ,cash_apply_date
                      ,MAX(loan_amount) loan_amount
                      ,MAX(cash_amount) cash_amount
                      ,SUM(principal) principal
                    FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                    LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                    WHERE apply_date<latest_apply_date
                          AND repayment_time <=latest_apply_date
                    GROUP BY a.jrid,cash_id,cash_apply_date
                )a
                GROUP BY jrid,cash_id
            )a
        ) a
        LEFT JOIN
        (
        SELECT jrid
              ,cash_apply_date
              ,row_number()over(partition by jrid ORDER BY cash_apply_date) rn
            FROM
            (
            SELECT jrid
                  ,cash_id
                  ,MIN(cash_apply_date) cash_apply_date
                FROM
                (
                SELECT a.jrid
                      ,cash_id
                      ,cash_apply_date
                    FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                    LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                    WHERE apply_date<latest_apply_date
                    GROUP BY a.jrid,cash_id,cash_apply_date
                )a
                GROUP BY jrid,cash_id
            )a
        ) b  ON a.jrid=b.jrid AND a.rn=b.rn+1
    )a
GROUP BY jrid;
"

#获取用户最近一笔贷款前借钱相关行为特征
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas5;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas5 AS
SELECT jrid
      ,MAX(repayment_time_diff) max_overdue_time_diff
      ,MIN(repayment_time_diff) min_overdue_time_diff
      ,AVG(repayment_time_diff) avg_overdue_time_diff
      ,MIN(repayment_time) first_overdue_date
      ,MAX(repayment_time) latest_overdue_date
    FROM
    (
    SELECT a.jrid
          ,datediff(a.repayment_time,b.repayment_time) repayment_time_diff
          ,a.repayment_time
    FROM
    (
    SELECT jrid
          ,repayment_time
          ,row_number()over(partition by jrid ORDER BY repayment_time DESC) rn
        FROM
        (
        SELECT jrid
              ,cash_id
              ,MIN(repayment_time) repayment_time
            FROM
            (
            SELECT a.jrid
                  ,cash_id
                  ,repayment_time
                FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                WHERE apply_date<latest_apply_date
            )a
            GROUP BY jrid,cash_id
        )a
    ) a
    LEFT JOIN
    (
    SELECT jrid
          ,repayment_time
          ,row_number()over(partition by jrid ORDER BY repayment_time DESC) rn
        FROM
        (
        SELECT jrid
              ,cash_id
              ,MIN(repayment_time) repayment_time
            FROM
            (
            SELECT a.jrid
                  ,cash_id
                  ,repayment_time
                FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                WHERE apply_date<latest_apply_date
            )a
            GROUP BY jrid,cash_id
        )a
    ) b  ON a.jrid=b.jrid AND a.rn=b.rn-1
    )a
GROUP BY jrid;
"

#获取用户最近一笔贷款前借钱相关行为特征
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
DROP TABLE if exists ft_tmp.dwd_wallet_crpl_user_fraud_feas6;
CREATE TABLE ft_tmp.dwd_wallet_crpl_user_fraud_feas6 AS
SELECT jrid
      ,MAX(apply_cash_date_diff) max_apply_cash_date_diff
      ,MIN(apply_cash_date_diff) min_apply_cash_date_diff
      ,AVG(apply_cash_date_diff) avg_apply_cash_date_diff
    FROM
    (
    SELECT jrid
          ,datediff(cash_apply_date,apply_date) apply_cash_date_diff
        FROM
        (
        SELECT jrid
              ,cash_id
              ,MIN(apply_date) apply_date
              ,MIN(cash_apply_date) cash_apply_date
            FROM
            (
            SELECT a.jrid
                  ,cash_id
                  ,apply_date
                  ,cash_apply_date
                FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas_orig a
                LEFT JOIN ft_tmp.dwd_wallet_crpl_user_fraud_feas4 b ON a.jrid=b.jrid
                WHERE apply_date<latest_apply_date
            )a
            GROUP BY jrid,cash_id
        )a
    )a
GROUP BY jrid;
"

#以上特征汇总
hive -S -e "
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
ALTER TABLE ft_tmp.dwd_cprl_antifraud_trans_feas DROP IF EXISTS PARTITION(dt='${calculate_day}');
INSERT overwrite table ft_tmp.dwd_cprl_antifraud_trans_feas partition (dt='${calculate_day}')
SELECT a.jrid jdpin
      ,max_monthly_income max_monthly_income
      ,min_monthly_income min_monthly_income
      ,highest_eduction highest_eduction
      ,accumulation_fund_auth accumulation_fund_auth -- 是否公积金授权
      ,id_card_auth id_card_auth
      ,operator_auth operator_auth
      ,lend_id_succ_cnt lend_id_succ_cnt
      ,apply_passed_cnt apply_passed_cnt
      ,overdue_days overdue_days
      ,lend_id_failed_cnt lend_id_failed_cnt
      ,apply_failed_cnt apply_failed_cnt
      ,loan_amount loan_amount
      ,cash_amount cash_amount
      ,round(overdue_principal,2) overdue_principal
      ,loan_amount_b
      ,cash_amount_b
      ,first_loan_amount first_loan_amount
      ,first_cash_amount first_cash_amount
      ,first_overdue_principal first_overdue_principal
      ,max_cash_apply_date_diff max_cash_apply_date_diff
      ,min_cash_apply_date_diff min_cash_apply_date_diff
      ,avg_cash_apply_date_diff avg_cash_apply_date_diff
      ,max_overdue_time_diff max_overdue_time_diff
      ,min_overdue_time_diff min_overdue_time_diff
      ,avg_overdue_time_diff avg_overdue_time_diff
      ,max_apply_cash_date_diff max_apply_cash_date_diff
      ,min_apply_cash_date_diff min_apply_cash_date_diff
      ,avg_apply_cash_date_diff avg_apply_cash_date_diff
      ,datediff(first_cash_apply_date,first_apply_date) first_cash_apply_date_diff
      ,datediff(first_overdue_date,first_cash_apply_date) first_overdue_cash_date_diff
      ,CASE WHEN first_cash_amount >0 THEN first_cash_amount/first_loan_amount ELSE NULL END first_cash_ratio
      ,CASE WHEN first_overdue_principal >0 THEN first_overdue_principal/first_cash_amount ELSE NULL END first_overdue_ratio
      ,first_apply_date first_apply_date
      ,latest_apply_date latest_apply_date
    FROM ft_tmp.dwd_wallet_crpl_user_fraud_feas1 a LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas2 b ON a.jrid=b.jrid LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas3 c ON a.jrid=c.jrid LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas4 d ON a.jrid=d.jrid LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas5 e ON a.jrid=e.jrid LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas6 f ON a.jrid=f.jrid LEFT JOIN
         ft_tmp.dwd_wallet_crpl_user_fraud_feas20 g ON a.jrid = g.jrid
;"
