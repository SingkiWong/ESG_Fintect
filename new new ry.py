import pandas as pd 
import numpy as np


# ================================================================/=============
# 5.4.2 完整版百度新闻数据挖掘系统 by 王宇韬
# =============================================================================

import requests
import re
import pymysql
import time


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0'}
keywords = ['环保', '污染', '处罚', '违规', '债务', '违约', '纠纷', '毁约', '拖欠', '安全性', '质量','投诉']
def baidu(company):	
	company_1=[company+' '+keyword for keyword in keywords]	
	for k_name in company_1:
		print(k_name)
		for i in range(5):
			num = i * 10					
			# 1.获取网页源代码（参考2.3、3.1、3.4节）			
			url='https://www.baidu.com/s?tn=news&rtt=4&bsst=1&cl=2&wd=%s&pn=%s'%(k_name,num)
			res = requests.get(url, headers=headers, timeout=10).text

			# 2.编写正则提炼内容（参考3.1节）
			p_href = '<h3 class="c-title">.*?<a href="(.*?)"'
			p_title = '<h3 class="c-title">.*?>(.*?)</a>'
			p_info = '<p class="c-author">(.*?)</p>'
			href = re.findall(p_href, res, re.S)
			title = re.findall(p_title, res, re.S)
			info = re.findall(p_info, res, re.S)

			# 3.数据清洗（参考3.1节）
			source = []  # 先创建两个空列表来储存等会分割后的来源和日期
			date = []
			for i in range(len(title)):
				title[i] = title[i].strip()
				title[i] = re.sub('<.*?>', '', title[i])
				info[i] = re.sub('<.*?>', '', info[i])
				source.append(info[i].split('&nbsp;&nbsp;')[0])
				date.append(info[i].split('&nbsp;&nbsp;')[1])
				source[i] = source[i].strip()
				date[i] = date[i].strip()

				# 统一日期格式（参考5.1节）
				date[i] = date[i].split(' ')[0]
				date[i] = re.sub('年', '-', date[i])
				date[i] = re.sub('月', '-', date[i])
				date[i] = re.sub('日', '', date[i])
				if ('小时' in date[i]) or ('分钟' in date[i]):
					date[i] = time.strftime("%Y-%m-%d")
				else:
					date[i] = date[i]

			# 4.舆情评分版本4及数据深度清洗（参考5.1和5.2和5.3节）
			score = []
			# keywords = ['环保', '污染', '处罚', '违规', '债务', '违约', '纠纷', '毁约', '拖欠', '安全性', '质量','投诉']
			for i in range(len(title)):
				num = 0
				try:
					article = requests.get(href[i], headers=headers, timeout=10).text
				except:
					article = '爬取失败'

				try:
					article = article.encode('ISO-8859-1').decode('utf-8')
				except:
					try:
						article = article.encode('ISO-8859-1').decode('gbk')
					except:
						article = article
				p_article = '<p>(.*?)</p>'
				article_main = re.findall(p_article, article)  # 获取<p>标签里的正文信息
				article = ''.join(article_main)  # 将列表转换成为字符串  
				for k in keywords:
					#if (k in article) or (k in title[i]):
					if (k in title[i]):
						num -= 5
				score.append(num)
				# 数据深度清洗（参考5.1节）
				company_re = company[0] + '.{0,5}' + company[-1]
				if len(re.findall(company_re, article)) < 1:
					title[i] = ''
					source[i] = ''
					href[i] = ''
					date[i] = ''
					score[i] = ''
			while '' in title:
				title.remove('')
			while '' in href:
				href.remove('')
			while '' in date:
				date.remove('')
			while '' in source:
				source.remove('')
			while '' in score:
				score.remove('')

			title_all=[	title[i] for i in range(len(title))]
			href_all= [href[i] for i in range(len(title))]
			source_all= [source[i] for i in range(len(title))]
			date_all= [date[i] for i in range(len(title))]
			score_all= [score[i] for i in range(len(title))]
			# 5.打印清洗后的数据（参考3.1节）
	for i in range(len(title_all)):
		print(str(i + 1) + '.' + title_all[i] + '(' + date_all[i] + ' ' + source_all[i] + ')')
		print(href_all[i])
		print(company + '该条新闻的舆情评分为' + str(score_all[i]))
###----------------写入数据库---------------###
	db = pymysql.connect(host='localhost', port=3306, user='root', password='', database='pachong', charset='utf8')
	cur = db.cursor()
	# 6.将数据存入数据库及数据去重（参考4.4节和5.1节）
	for i in range(len(title_all)):
		# db = pymysql.connect(host='localhost', port=3306, user='root', password='', database='pachong', charset='utf8')
		  # 获取会话指针，用来调用SQL语句
	#     sql_2 = 'INSERT INTO article(company,title,href,source,date,score) VALUES (%s,%s,%s,%s,%s,%s)'
	#     cur.execute(sql_2, (company, title[i], href[i], source[i], date[i], score[i]))  # 编写SQL语句
	#     db.commit()
	#     db.close()
	# print('------------------------------------')
	#     6.1 查询数据，为之后的数据去重做准备
		sql_1 = 'SELECT * FROM article_ESG WHERE company =%s'
		cur.execute(sql_1, company)
		data_all = cur.fetchall()
		title_all = []
		for j in range(len(data_all)):
			title_all_1.append(data_all[j][1])

		# 6.2 判断数据是否在原数据库中，不在的话才进行数据存储
		if title_all[i] not in title_all_1:
			sql_2 = 'INSERT INTO article_ESG(company,title,href,source,date,score) VALUES (%s,%s,%s,%s,%s,%s)'  # 编写SQL语句
			cur.execute(sql_2, (company, title_all[i], href_all[i], source_all[i], date_all[i], score_all[i]))  # 执行SQL语句
			db.commit()  # 当改变表结构后，更新数据表的操作
		  # 关闭会话指针
		# db.close()  # 关闭数据库链接
	#print('------------------------------------')  # 分割符
	cur.close()
	db.close() 


# 7.批量爬取多家公司（参考3.2节）
df=pd.read_excel('test.xlsx')
companies=np.array(df['证券简称']).tolist()
# companies=['万科']
# keywords = ['环保', '污染', '处罚', '违规', '债务', '违约', '纠纷', '毁约', '拖欠', '安全性', '质量','投诉']
# company_1=[]
# for keyword in keywords:
# 	company_1.append(companies[0]+''+keyword)

for company in companies:
    # try:
    baidu(company)
    print(company + '数据爬取并存入数据库成功')
    # except:
    #     print(company + '数据爬取并存入数据库失败')
# for company in companys:
# 	baidu(company)
# 	print(company + '数据爬取并存入数据库成功')