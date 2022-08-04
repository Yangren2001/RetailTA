# encoding = utf-8

"""
    @process: 预处理数据
"""

import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_162'
import hdfs

import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructField, StructType


class Preprocess:
    """
        预处理
    """

    def __init__(self):
        self.__client = None     # hadoop集群连接对象
        self.__sc = None          # SparkContext 对象
        self._df = None           # pyspark dataframe对象    
        
        # SPARK 配置
        self.conf = SparkConf().setMaster('local').setAppName("preprocess app")
        pass


    def connect(self, url):
        """
            连接hadoop集群
            :param url: hadoop集群web端地址
            :return : None
        """
        self.__client = hdfs.Client(url, root='/')
        print("Connect Success!")

    def upload(self, hdfs_path, path=None):
        """
          上传文件
          :param hdfs: hdfs路径
          :param path: 上传文件本地路径
          :return : None
        """
        if not self.__client:
            raise Exception("Not Connect to Hadoop")
        filepath = path
        if not path:
            path = '/data/data.csv'
            filepath = os.path.dirname(os.path.abspath(__file__)) + path
        if not os.path.isfile(filepath):
           raise Exception("file path error!")
        self.__client.upload(hdfs_path, filepath)
        print("upload Success!")
        print("info for file hdfs path:{}".format(self.__client.list(hdfs_path)))

    def download(self, hdfs):
        """
           下载文件
           :param hdfs: hdfs路径
           :param path: 下载到本地路径
           :return : None
        """
        if not self.__client:
            raise Exception("Not Connect to Hadoop")
        if not os.path.isdir(path):
            self.__client.close()
            raise Exception("file path error!")
        self.__client.download(hdfs_path, path)
        print("Download Success!")

    def dfGet(self, data_path):
        """
            读取hdfs中数据文件转化为dataframe
            ：param data_path: 数据文件路径 例：hdfs://localhost:9000/user/hadoop/data.csv
            :return : dataframe对象
        """
        self.__sc = SparkContext(conf=self.conf)
        self._df = SparkSession.builder.getOrCreate()
        self.__sc.setLogLevel('WARN')
        self._data = self._df.read.format("com.databricks.spark.csv").options(header=True, inferschema=True).load(data_path)
        print("dataset size:", self._data.count())
        print("dataset label attribute:")
        self._data.printSchema()
        # 建立临时表
        self._data.createOrReplaceTempView("data")
        #print("head 1000 row of data:", self._data.head(1000))
        # 过滤数据
        self.new_data=self._data.filter(self._data["CustomerID"]!=0).filter(self._data["Description"]!="")
        print("after filter of data size:", self.new_data.count()) 
        # 保存数据
        self.new_data.write.format("com.databricks.spark.csv").options(header=True, inferschema=True).save('/user/hadoop/new_data.csv')
        self.closeSpark()
   
    def closeSpark(self):
        """
            关闭pyspark对象
        """
        self.__sc.stop()
        self.__sc = None
        print("SparkContext close")

if __name__ == "__main__":
    ts = Preprocess()
    ts.connect('http://127.0.0.1:9870')
    ts.upload('user/hadoop/')
    ts.dfGet('hdfs://localhost:9000/user/hadoop/data.csv')
