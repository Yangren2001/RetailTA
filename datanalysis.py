# encoding = utf-8

import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/jdk1.8.0_162'
#memory = '10g'
#pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
#os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField
import json




class Analysis:
    """
          数据分析
    """
      
    def __init__(self, path=None):
        self._conf = SparkConf().setMaster('local').setAppName('Analysis app')             # SparkContext 配置
        self._pdate = None # 价格与日期 定为类成员减少数据运算以空间为代价
        if path:
            self.getData(path)
      
    def getData(self, path):
        """
              获取预处理后的数据
              :param path: 节点文件地址, 例：hdfs:loclhost:9000/user/hadoop/new_data.csv
        """
        self.__sc = SparkContext(conf=self._conf)
        self.__sc.setLogLevel("WARN")
        self._df = SparkSession.builder.getOrCreate()
        self._data = self._df.read.format("com.databricks.spark.csv").options(header=True, inferschema=True).load(path)
        self._data.createOrReplaceTempView('New_data')
      
    def addressAls(self):
        """
              客户居住地分析
              :return : data
        """
        address = self._df.sql("SELECT Country,COUNT(DISTINCT CustomerID) AS countOfCustomer FROM New_data GROUP BY Country ORDER BY countOfCustomer DESC")
        return address.collect()

    def goodsKeyWord(self):
        """
              频率最高的三百个商品关键词
              :return : data
        """
        key_word_num = self._df.sql("SELECT LOWER(Description) as description from New_data").rdd.flatMap(lambda x: x['description'].split(" ")).map(lambda word: (word,1)).reduceByKey(lambda num1, num2: num1 + num2).repartition(1).sortBy(lambda x: x[1], False)
        key_word_schema = StructType([StructField('word', StringType(), True), StructField('count', IntegerType(), True)])
        df = self._df.createDataFrame(key_word_num, key_word_schema)
        # 过虑空值
        word_count = df.filter(df['word'] != '')
        return word_count.take(300) 

    def stockCount(self):
        """
            销售最多的十个商品
            :return : data
        """
        stock_amount = self._df.sql("SELECT StockCode, SUM(Quantity) AS count FROM New_data GROUP BY StockCode ORDER BY count DESC LIMIT 10")
        return stock_amount.collect()

    def countryQuantity(self):
        """
            销量最多的十个国家
            : return : data
        """
        country = self._df.sql("SELECT Country, SUM(Quantity) AS count FROM New_data GROUP BY Country ORDER BY count DESC LIMIT 10")
        return country.collect()
    
    def formatTime(self, flag=True):
        """
            格式化时间
            :param flag:bool true: return day,false:return mouth
            :return : [year, mouth, day, price, num]
        """
        date_rdd = self._data.select("InvoiceDate", "Quantity", "UnitPrice").rdd
        data = date_rdd.map(lambda x: (x[0].split(" ")[0], x[2], x[1])).map(lambda x: (x[0].split("/"), x[1], x[2])).map(lambda x: (x[0][2], x[0][0] if len(x[0][0]) == 2 else '0' + x[0][0], x[0][1] if len(x[0][1]) == 2 else '0' + x[0][1], x[1], x[2]))
        return data

    def daySala(self):
        """
            日销量额
            :return : data
        """
        if not self._pdate:
            self._pdate = self.formatTime() # 价格与日期 定为类成员减少数据运算以空间为代价
        result1 = self._pdate.map(lambda x: (x[0] + '_'  +  x[1] + '_' + x[2], x[3] * x[4])).reduceByKey(lambda a,b: a + b).sortByKey()
        schema = StructType([StructField("date", StringType(), True), StructField("tradeprice", DoubleType(), True)])
        result = self._df.createDataFrame(result1, schema)
        return result.collect()

    def mouthsala(self):
        """
            月销售额
            :return : data
        """
        if not self._pdate:
            self._pdate = self.formatTime()
        result1 = self._pdate.map(lambda x: (x[0] +'_' +  x[1], x[3] * x[4])).reduceByKey(lambda a,b: a + b).sortByKey()
        schema = StructType([StructField("date", StringType(), True), StructField("tradeprice", DoubleType(), True)])
        result = self._df.createDataFrame(result1, schema)
        return result.collect()

    def returnGoods(self):
        """
            各国返回商品
            :return : data
        """
        return_ = self._df.sql("SELECT Country, COUNT(Quantity) AS amount FROM New_data WHERE InvoiceNo LIKE 'C%' GROUP BY Country ORDER BY amount DESC")
        return return_.collect()

    def buyGoods(self):
        """
           各国购买商品
           :return : data
        """
        return_ = self._df.sql("SELECT Country, COUNT(Quantity) AS amount FROM New_data WHERE InvoiceNo LIKE '[^C]%' GROUP BY Country ORDER BY amount DESC")
        return return_.collect()

    def save(self, path, data):
        """
            保存数据
            :param path: 文件路径
            :param data: 数据
            :return : None
        """
        with open(path, 'w') as f:
            f.write(data)

    def closeSpark(self):
        """
             关闭pyspark对象
        """
        self.__sc.stop()
        self.__sc = None
        print("SparkContext close")
		
    def __call__(self):
        base = os.path.dirname(os.path.abspath(__file__)) + '/static/' # 该文件所在目录
        if not os.path.isdir(base):
            os.mkdir(base)
        data_js = {
       		 "addressAls": {
            		"method": self.addressAls,
            		"path": "addressAls.json"
        		},
        	 "countryQuantity": {
            		"method": self.countryQuantity,
            		"path": "countryQuantity.json"
        		},
        	 "daySala": {
            		"method": self.daySala,
            		"path": "daySala.json"
        		},
         	 "stockCount": {
            		"method": self.stockCount,
            		"path": "stockCount.json"
        		},
        	 "goodsKeyWord": {
            		"method": self.goodsKeyWord,
            		"path": "goodsKeyWord.json"
        		},
        	 "mouthsala": {
            		"method": self.mouthsala,
            		"path": "mouthsala.json"
        		},
        	"returnGoods": {
            		"method": self.returnGoods,
            		"path": "returnGoods.json"
        		},
        	"buyGoods": {
            		"method": self.buyGoods,
            		"path": "buyGoods.json"
        	}
    	}
        for key in data_js.keys():
            print("正在执行{}数据写入".format(key))
            f = data_js[key]["method"]
            path = data_js[key]["path"]
            self.save(base + path, json.dumps(f()))
        print("数据写入完成")
        self.closeSpark()

if __name__ == "__main__":
    a = Analysis("hdfs://localhost:9000/user/hadoop/new_data.csv")
    a()
