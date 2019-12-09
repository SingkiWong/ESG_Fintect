import pymysql
# db = pymysql.connect(host='localhost', port=3306, user='root', password='', database='pachong', charset='utf8')
# cur = db.cursor()
# sql_1 = "UPDATE article_esg SET company = CASE WHEN company = '首钢' THEN '首钢股份' END WHERE company RLIKE '首钢$' "
# cur.execute(sql_1)
# db.commit()  # 当改变表结构后，更新数据表的操作
# cur.close()
# db.close() 


import pymysql
db = pymysql.connect(host='localhost', port=3306, user='root', password='', database='pachong', charset='utf8')
cur = db.cursor()
print("打开数据库")
sql_1 = "delete from article_esg  where title in (SELECT tmp.title FROM(select title from article_esg group by title having count(title) > 1)tmp) "
cur.execute(sql_1)
print("成功解决")
db.commit()  # 当改变表结构后，更新数据表的操作
cur.close()
db.close() 



list_1=['万科','特力','深振业','陕国投','南玻','京东方']
db = pymysql.connect(host='localhost', port=3306, user='root', password='', database='pachong', charset='utf8')
cur = db.cursor()
for i in list_1:
	sql_1 = "UPDATE article_esg SET company = CASE WHEN company = %s THEN %s END WHERE company RLIKE %s COLLATE utf8mb4_bin"%(i,i+'A',i+'$')
	print(sql_1)

		cur.execute(sql_1)	
cur.close()
db.close() 

