# -*- coding: utf-8 -*-
import sys
import numpy as np
import pandas as pd
import time
import math
import datetime
from datetime import datetime, timedelta
from scipy import stats
import pyspark.sql.types
from pyspark.sql import SQLContext
from pyspark.sql import functions as fn
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml import tuning as tune
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import rand
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler,MinMaxScaler
from pyspark.sql.types import StructField, StructType, FloatType, StringType, DoubleType,IntegerType
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier,GBTClassifier,DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

"""
@author: DR
@date: 2019-04-09
@desc: This script is used to training classification models for the project of antifraud
@question:
@版本参数说明：本版本最终模型使用的是全量标签数据，v1最终模型使用的是train
     $1: target，目标变量[is_fraud, is_fraud2]
     $2: del_target，不使用目标变量[is_fraud, is_fraud2]
     $3: 是否训练模型: 1-训练; 0-不训练,读取保存的模型
     $4: yesterday,模型训练时间,格式:%y%m%d
@example:
聚类欺诈人群: spark-submit fraud_score.py  is_fraud is_fraud2 1 20190505  --executor-memory 6g --executor-num 20  --master yarn 1>is_fraud_score.out 2>e.out
聚类欺诈人群修正：spark-submit fraud_score.py is_fraud2 is_fraud 0 20190505 --executor-memory 6g --executor-num 20  --master yarn 1>is_fraud2_score.out 2>e.out

"""

seed = 2019
#yesterday =datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')

def extract(row):
    """
    split vector into columns
    @example:df.rdd.map(extract).toDF()
    """
    return (row[IDcol],)+(row[target],) + tuple(row.probability.toArray().tolist())


def ks_calc(data,score_col,class_col):
    """计算KS值，输出对应分割点
    @param data: 二维数组或dataframe，包括模型得分和真实的标签
    @param score_col: 一维数组或series，代表模型得分（一般为预测正类的概率）
    @param class_col: 一维数组或series，代表真实的标签（{0,1}或{-1,1}）
    return: 字典，键值关系为{'ks': KS值，'split': KS值对应节点，'fig': 累计分布函数曲线图}

    """
    ks_dict = {}
    Bad = data.ix[data[class_col[0]]==1,score_col[0]]
    Good = data.ix[data[class_col[0]]==0, score_col[0]]
    ks,pvalue = stats.ks_2samp(Bad.values,Good.values)
    crossfreq = pd.crosstab(data[score_col[0]],data[class_col[0]])
    crossdens = crossfreq.cumsum(axis=0) / crossfreq.sum()
    crossdens['gap'] = abs(crossdens[0] - crossdens[1])
    score_split = crossdens[crossdens['gap'] == crossdens['gap'].max()].index[0]
    ks_dict['ks'] = ks
    ks_dict['split'] = score_split
    return ks_dict


def getData(df, IDcol, target, n_samp_ratio=[0.5,0.5]):
    """
    @param df: dataframe 数据
    @param IDcol: string jdpin
    @param target: string,y of model
    @param n_samp_ratio: array 正负样本采样比例
    return: 训练集和验证集
    """
    #分层采样
    df_sample = df.sampleBy(target, fractions={0: n_samp_ratio[0], 1: n_samp_ratio[1]}, seed=seed)
    a = df_sample.groupBy(target).count()
    b = a.sort(target).collect()
    good = b[1][1]
    bad = b[0][1]
    ratio = (good*1.0)/(good+bad)
    print('{sampleBy dataset: user number}:', good+bad)
    print('{sampleBy dataset: good}:',good)
    print('{sampleBy dataset: bad }:',bad)
    print('{sampleBy dataset: good ratio}:', ratio)
    df_sample = df_sample.na.fill(0)
    df_sample.cache()
    #训练集测试集划分
    # if sample_day=='2018-01-01':
    train,validation = df_sample.randomSplit([0.7, 0.3])
    # else:
        # train = df_sample.filter(df_sample.latest_cash_apply_date<sample_day)
        # validation = df_sample.filter(df_sample.latest_cash_apply_date>=sample_day)
    print('{train dataset: user number}:',train.count())
    print('{validation dataset: user number}:',validation.count())
    # train = train.drop('latest_cash_apply_date')
    # validation = validation.drop('latest_cash_apply_date')
    return train,validation

def trainModelTest(train, model):
    """
    @param train: dataframe 训练集
    @param model: 树模型
    return: dataframe 特征重要性
    """
    # features = [col for col in train.columns if col not in [IDcol, target]]
    # featureAssembler =VectorAssembler(inputCols=features, outputCol="features")
    # train = featureAssembler.transform(train)
    features = [col for col in train.columns if col not in [IDcol, target, 'features']]
    fi = model.featureImportances
    fi_list = []
    for i in range(0, len(features)):
        fi_list.append([features[i],np.float64(fi[i]).item()])
    df_feasi = spark.createDataFrame(fi_list, ["features", "importance"])
    df_feasi = df_feasi.sort(df_feasi.importance.desc())
    df_feasi = df_feasi.withColumn('importance',fn.round(df_feasi.importance,3))
    print('features importance are:')
    df_feasi.show()
    # train = train.drop('features')
    return df_feasi


def trainModel(IDcol, target, classifier, paramGrid, train, validation):
    """
    @param IDcol: string, jdpin
    @param target: string,y of model
    @param classifier: 实例化的分类模型
    @param paramGrid：分类模型对应的参数空间
    @param train: dataframe 训练集+验证集
    @param validation: dataframe 测试集
    return: 模型对应参数空间所能找到的最优模型
    """
    start_time = datetime.now()
    print('模型调参中 ...')
    data = train.union(validation) #尽量用群量数据训练模型
    evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol=target, metricName='areaUnderROC')
    cv = tune.CrossValidator(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator)
    features = [col for col in train.columns if col not in [IDcol, target]]
    featureAssembler = VectorAssembler(inputCols=features, outputCol="features")
    train = featureAssembler.transform(train)
    cvModel = cv.fit(train)
    # 提取模型最佳参数
    results = [([{key.name: paramValue}
        for key, paramValue in zip(params.keys(), params.values())], metric)
        for params, metric in zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics)]
    best_result = sorted(results, key=lambda el: el[1],reverse=True)[0]
    print('the best model is like this:', best_result)
    best_param = {}
    for param in best_result[0]:
        best_param =  dict( best_param, **param)
    best_model = classifier.setParams(**best_param)
    best_model = best_model.fit(train, best_param)
    df_feasi_tune = trainModelTest(train, best_model)
    # train = train.drop('features')
    # model_in = PipelineModel.load('{}_MODEL'.format(str(classifier).split('_')[0]))
    #'DecisionTreeClassifier'
    validation = featureAssembler.transform(validation)
    prediction = best_model.transform(validation)
    auc=evaluator.evaluate(prediction)
    print('the auc of {} in validation dataset is:'.format(str(classifier).split('_')[0]), auc)
    prediction = prediction.select(IDcol, target, "probability")
    # prediction = prediction.withColumnRenamed(target,'label')
    prediction = prediction.rdd.map(extract).toDF()
    for col1,col2 in zip(prediction.columns,[IDcol, target,'probability0','probability1']):
        prediction = prediction.withColumnRenamed(col1,col2)
    prediction = prediction.withColumn('probability1',fn.round(prediction.probability1,3))
    #prediction.write.mode("overwrite").saveAsTable('ft_tmp.crpl_{}_probability1_{}'.format(target,yesterday))
    prediction = prediction.toPandas()
    ks_dict = ks_calc(prediction,['probability1'],['is_fraud2'])
    print('the ks of {} in validation dataset is:'.format(str(classifier).split('_')[0]), ks_dict['ks'])
    print('the ks split of {} in validation dataset is:'.format(str(classifier).split('_')[0]), ks_dict['split'])
    #利用训练好的模型训练pipeline模型
    pipeline = Pipeline(stages=[featureAssembler, best_model])
    model = pipeline.fit(data)
    # model.save('{}_MODEL'.format(str(classifier).split('_')[0]))
    model.write().overwrite().save('{}_{}_MODEL'.format(str(classifier).split('_')[0], target))
    end_time = datetime.now()
    print('the time cost for model training is:', (end_time-start_time).seconds/60, 'min')


def results(data, classifier, target):
    """
    @param data: dataframe 模型预测数据
    @param classifier: 实例化的分类模型
    @param target: string,y of model
    return: dataframe 模型预测结果
    """
    #print('start saving prediction results for all users...')
    start_time = datetime.now()
    model = PipelineModel.load('{}_{}_MODEL'.format(str(classifier).split('_')[0], target))
    prediction = model.transform(data)
    prediction = prediction.select(IDcol, target, "probability")
    prediction = prediction.rdd.map(extract).toDF()
    for col1,col2 in zip(prediction.columns,[IDcol, target,'probability0','probability1']):
        prediction = prediction.withColumnRenamed(col1,col2)
    prediction = prediction.withColumn('probability1',fn.round(prediction.probability1,3))
    # prediction = prediction.withColumn('probability1', when(prediction.probability1==0,0.00001).otherwise(prediction.probability1))
    # prediction = prediction.withColumn('score',math.log(prediction.probability1/(1-prediction.probability1))*(20/math.log(2))+600)
    end_time = datetime.now()
    print('the time cost for geting results is:', (end_time-start_time).seconds/60, 'min')
    return prediction


if __name__ == '__main__':
    try:
        target = sys.argv[1]
        del_target = sys.argv[2]
        trainmodel_flag = sys.argv[3]
        yesterday = sys.argv[4]
        # sample_day = sys.argv[4]
        # flag = sys.argv[5]
    except KeyboardInterrupt:
        pass
    print('-----------------{}-----------------'.format(yesterday))
    #target = 'is_fraud2'
    #del_target = 'is_fraud'
    IDcol = 'jdpin'
    spark = SparkSession.builder.appName("user_cluster").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    data = spark.sql("select * from ft_tmp.wallet_crpl_user_fraud_label_{}".format(yesterday))
    # data = spark.sql("select * from ft_tmp.wallet_crpl_user_fraud_label_{}".format('20190428'))
    data = data.drop('dt').drop('cash_rate').drop('overdue_rate').drop('prediction')
    data_latest = data.filter(data.first_apply_date != data.latest_apply_date)
    data_latest = data_latest.drop(del_target)
    #data_latest = data_latest.filter(data_latest.overdue_days>3)
    print('The num of lending users is:',data_latest.count())
    # data_latest = data_latest.drop('min_overdue_time_diff').drop('avg_overdue_time_diff').drop('max_overdue_time_diff').drop('overdue_days')
    data_latest = data_latest.na.fill(0)
    #data_latest = data_latest.filter((data_latest[target]==1)|((data_latest[target]==0)&(data_latest.overdue_days<3)))
    #data_latest = data_latest.drop('overdue_days')
    data_latest = data_latest.drop('loan_amount').drop('cash_amount').drop('overdue_principal').drop('first_overdue_cash_date_diff')
    data_latest = data_latest.drop('first_apply_date').drop('latest_apply_date')
    data_latest = data_latest.drop('max_overdue_time_diff').drop('avg_overdue_time_diff').drop('min_overdue_time_diff')
    #data_latest = data_latest.drop('loan_amount_b').drop('cash_amount_b')
    data_latest = data_latest.drop('loan_amount_b')
    #data_latest = data_latest.na.fill(0)

    # data_train = data.filter(data['latest_cash_apply_date']>'2018-02-28')
    # 排灰
    # data_paihui = data_latest.filter((data_latest[target]==1)|((data_latest[target]==0)&(data_latest.overdue_days<3)))
    # data_paihui = data_paihui.drop('overdue_days')
    print('The num of lending users after filtering grey users is:',data_latest.count())
    a = data_latest.groupBy(target).count()
    b = a.sort(target).collect()
    good = b[1][1]
    bad = b[0][1]
    ratio = (good*1.0)/(good+bad)
    print('{original dataset: user number}:',data_latest.count())
    print('{original dataset: good}:',good)
    print('{original dataset: bad }:',bad)
    print('{original dataset: good ratio}:', ratio)
    classifier = RandomForestClassifier(featuresCol='features', labelCol=target, cacheNodeIds=True, seed=seed)
    data_latest = data_latest.drop('overdue_days')
    if trainmodel_flag== '1':
        good_ratio = 1
        bad_ratio = ratio*3
        n_samp_ratio = [bad_ratio, good_ratio]
        train,validation = getData(data_latest, IDcol, target, n_samp_ratio=n_samp_ratio)
        # features= [col for col in train.columns if col not in [IDcol, target]]
        # classifier = RandomForestClassifier(featuresCol='features', labelCol=target, cacheNodeIds=True, seed=seed)
        # classifier_paramGrid = ParamGridBuilder().addGrid(classifier.maxDepth, [10,15,30]) \
        #                                  .addGrid(classifier.numTrees, [50,100]) \
        #                                  .addGrid(classifier.minInstancesPerNode, [5, 10]) \
        #                                  .build()
        classifier_paramGrid = ParamGridBuilder().addGrid(classifier.maxDepth, [15]) \
                                         .addGrid(classifier.numTrees, [100]) \
                                         .addGrid(classifier.minInstancesPerNode, [5]) \
                                         .build()
        print('training models ...')
        trainModel(IDcol, target, classifier, classifier_paramGrid, train, validation, yesterday)
        prediction = results(data_latest, classifier, target, yesterday)
    elif trainmodel_flag== '0':
        prediction = results(data_latest, classifier, target, yesterday)
    else:
        print('Please input train_model_flag 1 for training or 0 for load model...')
    prediction_new = prediction.join(data.select(['jdpin','overdue_days','is_fraud','latest_apply_date','first_apply_date']),prediction.jdpin==data.jdpin).drop(data.jdpin)
    prediction_new.write.mode("overwrite").saveAsTable('ft_tmp.crpl_fraud_probability1_all_{}'.format(yesterday))


