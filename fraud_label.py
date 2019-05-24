# -*- coding: utf-8 -*-
import sys
import numpy as np
import pandas as pd
import time
import math
import datetime
from pyspark.sql import SparkSession
from sklearn.externals import joblib
from pyspark.sql import functions
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,DoubleType
from pyspark.ml.clustering import KMeans,KMeansModel
from pyspark.sql.functions import split, explode, concat, concat_ws, when
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler,MinMaxScaler

"""
@author: DR
@date: 2019-04-09
@desc: This script is used to label fraud users for the project of antifraud
@question:
@版本参数说明：
     $1: target，目标变量[is_fraud, is_fraud2]
     $2: overdue_days,聚类用户呆帐最长时间
     $3:trainmodel_flag,是否训练模型： 1-训练; 0
     $4:dt读取欺诈定义特征数据时间
     $5:today,格式 %Y%m%d
@example:
spark-submit fraud_label.py is_fraud2 31 0 2019-04-28 20190428--executor-memory 6g --executor-num 20  --master yarn 1>>label_results.out 2>e.out
"""
reload(sys)
sys.setdefaultencoding('utf-8')

#全局变量
seed = 12345
IDcol = 'jdpin'


#欺诈标签
def get_fraud_label(data, od_days, trainmodel_flag, today):
    """mark data with fraud labels
    @param data: DataFrame 打标数据
    @param od_days: int 聚类样本群体
    @param trainmodel_flag: int 是否训练模型
    @param sample: string 数据存储时类别标记
    return:
    """
    data = data.withColumn( "cash_rate", data.cash_amount/data.loan_amount)
    data = data.withColumn( "cash_rate", functions.round(data.cash_rate,3))
    data = data.withColumn( "overdue_rate", data.overdue_principal/data.cash_amount)
    data = data.withColumn( "overdue_rate", functions.round(data.overdue_rate,3))
    data = data.na.fill(0)
    m3_clu = data.filter(data.overdue_days>od_days)
    print('The num of clustering data is: ', m3_clu.select(IDcol).distinct().count())
    m3_clu = m3_clu.filter(m3_clu.overdue_principal>0)
    m3_clu = m3_clu.withColumn('overdue_rate', when(m3_clu.overdue_rate>1,1).otherwise(m3_clu.overdue_rate))
    print('The num of clustering data after filtering overdue_principal less 0 is: ', m3_clu.select(IDcol).distinct().count())
    featureAssembler =VectorAssembler(inputCols=['cash_rate','overdue_rate'], outputCol="features")
    m3_clu = featureAssembler.transform(m3_clu)
    if trainmodel_flag=='1':
        kmeans = KMeans(k=4, seed=2019)
        model = kmeans.fit(m3_clu)
        model.write().overwrite().save("kmeans_fraud") #版本1的对应名称：kmeans_fraud_v1
    else:
        model = KMeansModel.load("kmeans_fraud")
    m3_clu = model.transform(m3_clu)
    m3_clu = m3_clu.drop('features')
    m3_clu = m3_clu.withColumn('is_fraud', functions.lit(0))
    m3_clu = m3_clu.withColumn('is_fraud', when(m3_clu.prediction==0,1).otherwise(0))  #标记为欺诈用户
    m3_clu = m3_clu.withColumn('is_fraud2', functions.lit(0))
    m3_clu = m3_clu.withColumn('is_fraud2', when(m3_clu.is_fraud==1,1).otherwise(0))  #标记为欺诈用户
    print('the num of is_fraud users is:', m3_clu.filter(m3_clu.is_fraud==1).count())
    #欺诈定义修改
    m3_clu = m3_clu.withColumn('is_fraud2', when((m3_clu.is_fraud2==1)&(m3_clu.overdue_principal<=5000),0).otherwise(m3_clu.is_fraud2))
    m3_clu = m3_clu.withColumn('is_fraud2', when((m3_clu.is_fraud2==0)&(m3_clu.overdue_principal>20000),1).otherwise(m3_clu.is_fraud2))
    print('the num of is_fraud2 users is:',m3_clu.filter(m3_clu.is_fraud2==1).count())
    #全量数据打标
    m3_no_clu = data.filter(data.overdue_days<=od_days)
    print('The num of no clustering data is: ', m3_no_clu.select(IDcol).distinct().count())
    m3_no_clu = m3_no_clu.withColumn('prediction', functions.lit(-1))
    m3_no_clu = m3_no_clu.withColumn('is_fraud', functions.lit(0))
    m3_no_clu = m3_no_clu.withColumn('is_fraud2', functions.lit(0))
    data_users = m3_clu.union(m3_no_clu)
    # data_users 写表
    data_users.write.mode("overwrite").saveAsTable('ft_tmp.wallet_crpl_user_fraud_label_{}'.format(today))

# m3_clu.groupby('prediction').count().show()

if __name__ == '__main__':
    try:
        target = sys.argv[1]
        overdue_days= sys.argv[2]
        trainmodel_flag = sys.argv[3]
        dt = sys.argv[4]
        today = sys.argv[5]
    except KeyboardInterrupt:
        pass
    print('----------{}------------'.format(today))
    #聚类数据
    spark = SparkSession.builder.appName("user_cluster").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    data = spark.sql("select * from ft_tmp.dwd_cprl_antifraud_trans_feas where dt='{}'".format(dt))
    #data = data.withColumn('overdue_days', functions.lit(32))
    get_fraud_label(data, overdue_days, trainmodel_flag, today)
