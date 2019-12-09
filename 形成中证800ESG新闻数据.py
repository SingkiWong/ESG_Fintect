import pandas as pd
z800 = pd.read_excel("抓穷名单名单.xlsx",sheet_name='匹配名单')
# print(z800.head())
comli = list(z800['company'])
# cominfo = pd.DataFrame(columns=['company','E','S','G','ESG总和'])
# print(cominfo)



E_scandal = pd.read_excel("12.7中证800负面E新闻汇总.xlsx")
S_scandal = pd.read_excel("12.7中证800负面S新闻汇总.xlsx")
G_scandal = pd.read_excel("12.7中证800负面G新闻汇总.xlsx")
count = 0
for company in comli:
    try:
        num = E_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    z800.loc[count,'E'] = num
    count+=1
count = 0
for company in comli:
    try:
        num = S_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    z800.loc[count,'S'] = num
    count+=1
count = 0
for company in comli:
    try:
        num = G_scandal.groupby(['company']).size()['%s'%company]
    except:
        num = 0
    z800.loc[count,'G'] = num
    count+=1
z800.to_excel('中证800负面新闻数据条数汇总.xlsx')