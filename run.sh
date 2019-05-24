#!/bin/bash
source /etc/profile
source ~/.bash_profile
#Created on 20190430
#author: dingru1
#this script is used to control for running antifraud models,contains:
#step1.trans_feas.sh
#step2.fraud_label.py: 根据欺诈用户特征数据利用聚类算法标记用户是否为欺诈用户:{is_fraud:聚类结果欺诈用户,is_fraud2:聚类结果修正欺诈用户}
#step3.fraud_score.py: 训练随机森林模型评估用户欺诈评分
#step4.sql3: 用户欺诈评分结果存分区表,ft_tmp.dwd_wallet_crpl_user_fraud_probability

#设置定时任务：
#10 06 * * * ./etc/profile;/bin/sh /exportfs/home/dingru1/RISKMODEL/anti_fraud/fraud_score_v3/run.sh &> /exportfs/home/dingru1/RISKMODEL/anti_fraud/fraud_score_v3/run.out


#全局变量
# today=`date +"%Y%m%d"`
yesterdayv1=`date +"%Y%m%d" -d "-1 days"`
yesterday=`date +"%Y-%m-%d" -d "-1 days"`
yesterdayago1=`date +"%Y%m%d" -d "-2 days"`

overdue_days=31             #欺诈打标的聚类目标人群，当前为逾期天数31天以上
fraud_label=is_fraud2       #欺诈标签：is_fraud聚类模型标记出的欺诈人群，is_fraud2表示对is_fraud进行欺诈修正的结果标签
del_fraud=is_fraud
train_cluster_model_flag=0  #是否训练欺诈打标聚类模型，0读取保存模型，1重新训练模型
train_score_model_flag=0    #是否训练欺诈评分分类模型，0读取保存模型，1重新训练模型

# yesterdayv1=20190505
# yesterday=2019-05-05
# yesterdayago1=20190504

#最终校准结果写入评分表
sql1="
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
CREATE TABLE IF NOT EXISTS dwd_cprl_antifraud_lending_score(
jdpin                    string     COMMENT    'jdpin',
overdue_days             bigint     COMMENT    '当前逾期天数',
latest_apply_date        string     COMMENT    '最近一次申请时间',
is_fraud2                bigint     COMMENT    '欺诈修正',
probability1             double     COMMENT    '欺诈概率',
score                    double     COMMENT    '欺诈评分',
first_apply_date         string     COMMENT    '首次提现时间'
)
COMMENT '贷中用户反欺诈评分' PARTITIONED BY (dt STRING COMMENT '操作日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

;"

sql2="
SET hive.support.quoted.identifiers=None;
USE ft_tmp;
ALTER TABLE dwd_cprl_antifraud_lending_score DROP IF EXISTS PARTITION(dt='${yesterday}');
INSERT OVERWRITE TABLE dwd_cprl_antifraud_lending_score PARTITION(dt='${yesterday}')
SELECT jdpin
      ,latest_apply_date
      ,overdue_days
      ,is_fraud
      ,is_fraud2
      ,probability1
      ,CASE WHEN probability1=1 THEN 800
            WHEN probability1=0 THEN 400
            ELSE ln(probability1/(1-probability1))*(20/ln(2))+600 END score
      ,first_apply_date
    FROM ft_tmp.crpl_fraud_probability1_all_${yesterdayv1}
;"


step1 () {
    #step1:执行define_fraud_users.sh
    cnt=`hive -S -e "select count(*) from ft_tmp.dwd_cprl_antifraud_trans_feas where dt='${yesterday}'"`
    if [[ $cnt -lt 1 ]]; then
        sh trans_feas.sh $yesterday
        echo $?
    else
        echo "moved 0"
    fi
}

step2 () {
    #step2:执行fraud_label.py
    hive -S -e "drop table ft_tmp.wallet_crpl_user_fraud_label_${yesterdayago1}"
    cnt=`hiveless -S -e "select * from ft_tmp.wallet_crpl_user_fraud_label_${yesterdayv1}"`
    if [[ $cnt -lt 1 ]]; then
        /soft/client/spark-2.1.1-bin-2.6.0/bin/spark-submit fraud_label.py $fraud_label $overdue_days $train_cluster_model_flag $yesterday $yesterdayv1 --executor-memory 6g --executor-num 20  --master yarn 1>> label_results.out 2>e.out
        echo $?
    else
        echo "moved 0"
    fi
}

step3 () {
    #step2:执行fraud_label.py
    cnt=`hive -S -e "select count(*) from ft_tmp.dwd_cprl_antifraud_lending_score WHERE dt='${yesterday}'"`
    if [[ $cnt -lt 1 ]]; then
        /soft/client/spark-2.1.1-bin-2.6.0/bin/spark-submit fraud_score.py $fraud_label $del_fraud $train_score_model_flag $yesterdayv1 --executor-memory 6g --executor-num 20  --master yarn 1>> score_results.out 2>e.out
        echo $?
    else
        echo "moved 0"
    fi
}


#第一步
start_time=`date +%s`
state1=`step1| awk '{print $NF}' | awk '{print $NF}'`   #第一步运行结果状态码
cnt=`hive -S -e "select count(*) from ft_tmp.dwd_cprl_antifraud_trans_feas where dt='${yesterday}'"`
end_time=`date +%s`
echo "step1...time cost...: $((end_time-start_time)) s"
#后续步骤
if [[ $cnt -gt 1 ]]; then
    echo "step1:trans_feas.sh succeed!"
    start_time=`date +%s`
    state2=`step2 | awk '{print $NF}' | awk '{print $NF}'` #第二步运行结果状态码
    echo "state: $state2"
    end_time=`date +%s`
    echo "step2...time cost...: $((end_time-start_time)) s"
    cnt=`hive -S -e "select count(*) from ft_tmp.wallet_crpl_user_fraud_label_${yesterdayv1}"`
    if [[ $cnt -gt 1 ]]; then
        echo "step2:fraud_label.py succeed"
        start_time=`date +%s`
        state3=`step3 | awk '{print $NF}'` #第三步运行结果状态码
        echo "state: $state3"
        end_time=`date +%s`
        echo "ste3...time cost...: $((end_time-start_time)) s"
        cnt=`hive -S -e "select count(*) from ft_tmp.crpl_is_fraud2_probability1_all_$yesterdayv1"`
        if [[ $cnt -gt 1 ]]; then
            echo "step3:fraud_score.py succeed"
            start_time=`date +%s`
            hive -S -e "$sql1" 
            hive -S -e "$sql2"
            end_time=`date +%s`
            echo "step4...time cost...: $((end_time-start_time)) s"
            cnt=`hive -S -e "select count(*) from ft_tmp.dwd_cprl_antifraud_lending_score WHERE dt='${yesterday}'"`
            if [[ $cnt -gt 1 ]]; then
                echo "step4:save results succeed"
            else
                echo "step4:failed"
            fi
        else 
            echo "step3:failed"
        fi
    else 
        echo "step2:failed"
    fi
else 
    echo "step1:failed"
fi



