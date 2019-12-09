import pandas as pd
import pymysql
import datetime



db = pymysql.connect(host='localhost',user='root',password='',port=3306,db='panchong',charset='utf8')
cursor = db.cursor()

# 合并两个文件并去重
sql_a = 'SELECT * FROM `article`'
cursor.execute(sql_a)
db_1 = pd.DataFrame(cursor.fetchall())

sql_b = 'SELECT * FROM `article_esg`'
cursor.execute(sql_b)
db_2 = pd.DataFrame(cursor.fetchall())

df = db_1.append(db_2)
df = df.drop_duplicates()
db.commit()
db.close()


# 从excel文件提取去除词
delwd = pd.read_excel('去除词_1117.xlsx')
delwd_list = delwd.iloc[:,0]
# print(type(delwd_list))
delwd_list=list(delwd_list)
a="加强"
delwd_list.append(a)
# print(delwd_list)





# 用去除词筛选database中的新闻得到负面新闻库
condition = "|".join(delwd_list)

df.columns = ['company','title','source','date','href','score']
clean_database = df[df['title'].str.contains(condition)]
final_db = df.drop(clean_database.index)
# print(final_db.shape)


# '涉诉','伪造','违规','骗局','状告','缩水','蒸发','假冒','禁用','黑名单','事故','风控','爆雷','死伤事故'

# 从负面新闻库分出E、S、G新闻
E_word = ['环保','污染','处罚','违规']
S_word =['涉诉','伪造','违规','骗局','状告','缩水','蒸发','假冒','禁用','黑名单','事故','风控','爆雷','死伤事故']
G_word =['罚','贪','腐','辞职','舞弊']
E_condition = "|".join(E_word)
S_condition = "|".join(S_word)
G_condition = "|".join(G_word)
E_scandal = final_db[final_db['title'].str.contains(E_condition)]
S_scandal = final_db[final_db['title'].str.contains(S_condition)]
G_scandal = final_db[final_db['title'].str.contains(G_condition)]
ESG_scandal = E_scandal.append(S_scandal.append(G_scandal)).reset_index(drop=True)

s_date = datetime.datetime.strptime('20190101', '%Y%m%d').date()
ESG_scandal = ESG_scandal[ESG_scandal['date']>s_date]

ESG_scandal.to_excel('11.21负面新闻汇总.xlsx',encoding = 'utf-8')


# 得到沪深300公司列表
cominfo = pd.read_excel('沪深300人工与爬虫对比.xlsx')
# print(cominfo.head())
companys = list(cominfo['证券简称'])
# print(companys)


# 重新生成数据
count = 0
for company in companys:
    try:
        num = E_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    cominfo.loc[count,'E_爬虫'] = num
    count+=1
count = 0
for company in companys:
    try:
        num = S_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    cominfo.loc[count,'S_爬虫'] = num
    count+=1
count = 0
for company in companys:
    try:
        num = G_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    cominfo.loc[count,'G_爬虫'] = num
    count+=1
# print(cominfo.head(10))
cominfo['E_delta'] = cominfo['E_爬虫'] - cominfo['E_人工']
cominfo['S_delta'] = cominfo['S_爬虫'] - cominfo['S_人工']
cominfo['G_delta'] = cominfo['G_爬虫'] - cominfo['G_人工']
cominfo['ESG_delta'] = cominfo['E_delta']+cominfo['S_delta']+cominfo['G_delta']
# print(cominfo.head(10))
cominfo.to_excel('11.21更新沪深300人工与爬虫对比.xlsx')


















