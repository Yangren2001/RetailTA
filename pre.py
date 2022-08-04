import pandas as pd
import numpy as np
from sklearn import tree
from sklearn.feature_extraction import DictVectorizer
from sklearn import preprocessing

df_data_1 = pd.read_csv("C://Users/Administrator/Desktop/RetailTA/data/newdata.csv").drop(["InvoiceNo", "StockCode", "Description"], axis=1)
exc_cols = [u'Country']
cols = [c for c in df_data_1.columns if c not in exc_cols]
X_data = (df_data_1.loc[:,cols])[:3000]
y_data = df_data_1[u'Country'].values[:3000]
dict_X_train = X_data.to_dict(orient='records')#列表转换为字典
vec = DictVectorizer()
X_data = vec.fit_transform(dict_X_train).toarray()

X_train = X_data[:1300,:]
X_test = X_data[1300:,:]
y_train = y_data[:1300]
y_test = y_data[1300:]
lb = preprocessing.MultiLabelBinarizer()
dict_y_train = lb.fit_transform(y_train)
clf = tree.DecisionTreeClassifier(criterion='gini')
clf = clf.fit(X_train,y_train)
predictedY = clf.score(X_test,y_test)
s = clf.predict(X_test)
s1 = np.unique(s)
d = {}
for i in s1:
    d[i] = sum(s == i)

e = sorted(d.keys(), key=lambda x: x[1])
f = open("result.txt", 'w')
print("消费者最多的国家：", e[0], file=f)
f.close()
