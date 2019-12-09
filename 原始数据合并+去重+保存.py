import pandas as pd
import pymysql

# 获得原始数据+筛选+拼接
columns = ['company','title','source','date','time','href']
news1 = pd.read_csv('公司新闻.txt',names = columns,encoding='utf-8',header=None,sep = '\t')
news1 = news1.drop(['time'],axis=1)
# print(news1.shape)
news1 = news1[news1['date']>"2019年1月1日"]
# print(news1.shape)
# print(news1.head())


news2 = pd.read_csv('公司新闻+关键词+时间顺序.txt',names = columns,encoding='utf-8',header=None,sep = '\t')
news2 = news2.drop(['time'],axis=1)
# print(news2.shape)
news2 = news2[news2['date']>"2019年1月1日"]
# print(news2.shape)

# db = pymysql.connect(host='localhost',user='root',passwd='',port=3306,db='pachong')
# cursor = db.cursor()
# # sql1 = 'SELECT * FROM `article_esg` WHERE `date`>2019-01-01'
# sql1 = 'SELECT * FROM `article_esg` '
# cursor.execute(sql1)
# news3 = pd.DataFrame(cursor.fetchall(),columns = ['company','title','source','date','href','score'])
# # print(news3.head(10))
# news3 = news3.drop(['score'],axis=1)
# news3['date'] = news3['date'].astype('str')
# news3 = news3[news3['date']>'2019-01-01']
# # print(news3.shape)
# db.commit()
# db.close()
columns_1=['company','title','source','date','href','score']
news3 = pd.read_csv('2019-800.csv',names = columns_1,encoding='utf-8',header=None,sep = ',')
news3 = news3.drop(['score'],axis=1)
# print(news2.shape)

news = news3.append(news2.append(news1))
news = news.drop_duplicates()
# print(type(news))

news.to_csv('2019年中证800新闻库.csv')

