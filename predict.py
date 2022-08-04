# encoding = utf-8

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer,IndexToString

class Predict:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('predict app')
        pass

    def getData(self):
        self.__sc = SparkContext(conf=self.Conf)
        self.__sc.setLogLevel('WARN')
        self._df = SparkSession.builder.getOrCreate()
        self._data = self._df.read.format('com.databricks.csv').options(header=True, inferschema=True).load('hdfs://user/hadoop/new_data.csv')
        self._data.createOrReplaceTempView("data")

    def splitdata(self, data):
        """
            划分数据集
        """
        return data.randomSplit([0.7, 0.3])
        
    def decisionTree(self):
        """
            决策树
        """
        x_data = self._data.drop(['Description', 'StockCode', 'nvoiceNo', 'InvoiceDate'])
        y_data = self._data[:, 'Country']
        labelIndexer = StringIndexer(inputCol="Country", outputCol="CountryIndex").fit(y_data)
        featureIndexer=VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(x_data)
        trainingData, testData = self.splitdata(self._data)
    #模型训练
        dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol='featureIndexer')
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])
        model = pipeline.fit(trainingData)
        predictions = model.transform(testData)  #预测
        
        prd = predictions.select("prediction").map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).sortByKey(lambda x: x[1])
        return prd.take(1)

