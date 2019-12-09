a=['环保','污染','排污']
b=['罚','违规','问题','查处','点名','门','批评','通报','乱象','问责','无效','举报','消极','约谈','不良']
c=['污水']
d=['直排','渗透','未经处理','排放','渗排']
sql_list=[]

for i in a:
	for j in b:
		k="select * from `article_esg` where(`date`> '2019-01-01') and (`title` regexp '([%s].*[%s]+)|([%s].*[%s]+)'))"%(i,j,j,i)
		print(k)
		sql_list.append(k)        

for m in c:
	for n in d:
		k="select * from `article_esg` where(`date`> '2019-01-01') and (`title` regexp '([%s].*[%s]+)|([%s].*[%s]+)'))"%(m,n,n,m)
		print(k)
		sql_list.append(k)
		
print(sql_list)



