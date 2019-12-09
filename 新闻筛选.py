import pandas as pd



db = pd.read_csv('2019年中证800新闻库.csv')
# print(db.columns)
db = db.drop(['Unnamed: 0'],axis=1)
# print(db.shape)
# print(db.head())




# 用去除词初步筛选
delwd = pd.read_excel('去除词_1121.xlsx')
delwd_list = delwd.iloc[:,0]
# print(type(delwd_list))
delwd_list=list(delwd_list)
a="加强"
delwd_list.append(a)
print(delwd_list)
# print(delwd_list)
condition = "|".join(delwd_list)
# db_del = db[db['title'].str.contains(condition)]
# final_db = db.drop(db_del.index)
final_db=db[~db['title'].str.contains(condition)]
# print(final_db.shape)
final_db.drop_duplicates(subset=['company','title'],keep='first')
# print(final_db.shape)


# 用ESG关键词进一步筛选

# 社会责任负面新闻
S_words = ['山寨','伪造','骗','假冒','黑榜','黑名单','事故','抱负','纠纷','造假','虚假','虚增','欺诈','事故']
S_condition = "|".join(S_words)
# print(S_condition)
S_scandal1 = final_db[final_db['title'].str.contains(S_condition)]

S_B_words = ['问题','危机','事故','不合格','没保障','差','隐忧','存疑','担忧','堪忧']
S_A_words = ['安全性','质量']
li=[]
for a in S_A_words:
    for b in S_B_words:
        li.append("(("+a+")"+".*"+"("+b+")+)")
        li.append("((" + b + ")" + ".*" + "(" + a + ")+)")
S_AB = "|".join(li)
# print(S_AB)

S_scandal2 = final_db[final_db['title'].str.contains(S_AB, regex=True)]
S_scandal = S_scandal1.append(S_scandal2).drop_duplicates()
S_scandal.to_excel('12.7中证800负面S新闻汇总.xlsx',encoding = 'utf-8')
# print(S_scandal.head())
# print(S_scandal.shape)










# 公司治理负面新闻

G_words = ['罚','违约','爽约','毁约','违规','违法','刑拘','隐瞒','欠债','失责','失职','行贿','获刑']
G_condition = "|".join(G_words)
# print(G_condition)
G_scandal1 = final_db[final_db['title'].str.contains(G_condition)]

G_B_words = ['问题','危机','压','逾期','爆雷','千','亿','万','严重','缠身','担忧','影响','堪忧','巨大','巨额']
G_A_words = ['债务']
li=[]
for a in G_A_words:
    for b in G_B_words:
        li.append("(("+a+")"+".*"+"("+b+")+)")
        li.append("((" + b + ")" + ".*" + "(" + a + ")+)")
G_AB = "|".join(li)
# print(G_AB)

G_scandal2 = final_db[final_db['title'].str.contains(G_AB, regex=True)]

G_scandal = G_scandal1.append(G_scandal2).drop_duplicates()
G_scandal.to_excel('12.7中证800负面G新闻汇总.xlsx',encoding = 'utf-8')





# 环境保护负面新闻

EA=['环保','污染','排污','排放']
EB=['罚','违规','问题','查处','点名','门','批评','通报','乱象','问责','无效','举报','消极','约谈','不良','欺骗','谎报','超标','弄虚作假','典型案例']
EC=['污水']
ED=['直排','渗透','未经处理','排放','渗排']
li=[]
for a in EA:
    for b in EB:
        li.append("(("+a+")"+".*"+"("+b+")+)")
        li.append("((" + b + ")" + ".*" + "(" + a + ")+)")
for c in EC:
    for d in ED:
        li.append("(("+c+")"+".*"+"("+d+")+)")
        li.append("((" + d + ")" + ".*" + "(" + c + ")+)")

E_condition = "|".join(li)
E_scandal = final_db[final_db['title'].str.contains(E_condition, regex=True)]
E_scandal = E_scandal.drop_duplicates()
E_scandal.to_excel('12.7中证800负面E新闻汇总.xlsx',encoding = 'utf-8')


# 合并ESG数据并进一步处理
ESG_scandal = E_scandal.append(S_scandal.append(G_scandal)).reset_index(drop=True)
ESG_scandal.drop_duplicates()
ESG_scandal.to_excel('12.7中证800负面新闻汇总.xlsx',encoding = 'utf-8')










