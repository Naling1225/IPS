import pandas as pd
import mariadb
import sys
import pymysql
from sqlalchemy import create_engine


try:
    conn = mariadb.connect(
        user="admin",
        password="ips12341234",
        host= "ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com",
        port=3306,
        database="ips"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)


def classify(user):
    if (user['life_style'] == "day").bool():
        cate = 'D'
    else:
        cate = 'N'
    if (user['relation'] == "solo").bool():
        cate += 'S'
    else:
        cate += 'F'
    if (user['rest_style'] == "home").bool():
        cate += 'H'
    else:
        cate += 'O'
    return cate
pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
# cur = conn.cursor()
# cur.execute('select * from characteristic')
# res = cur.fetchall()
# users = pd.DataFrame.from_records(res)
users= pd.read_sql("SELECT * FROM characteristic;",engine,index_col='id')
print(users)
# #users = pd.read_csv('users.csv', index_col='name')
pref = pd.read_csv('preference.csv', index_col='Unnamed: 0')
char = []
# # print(classify(users.loc[users.index=='Jenny']))
print(users.index)
for name in users.index:
    char.append(classify(users.loc[users.index == name]))
#
users['char'] = char

print(users)
# users.to_csv("C:/Users/user/PycharmProjects/IPS/users.csv",sep=',',index=True)
random_user = users.sample(frac=1 / 15, random_state=60)
print(random_user)

print(classify(random_user))
prob = pref.loc[pref.index == classify(random_user)]
prob = prob.sort_values(by=classify(random_user), axis=1, ascending=False)
print(prob)
order = prob.columns.values.tolist()
print(order)
rtv = []
for i in order:
    j = 0
    for j in range(len(users) - 1):
        if users.iloc[j, 5] == i:
            print(users.iloc[j].name)
            rtv.append(users.iloc[j].name)
        j = j + 1
db=pd.DataFrame(data=rtv, columns=['id'])
char=[]
for i in db['id'].to_list():
    char.append(users.at[i,'char'])
db['char']=char
probb=[]
for j in db['char'].to_list():
    probb.append(prob.at[classify(random_user),j])
db['prob']=probb

db=db.drop('char', axis='columns')
print(db)

pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
db.to_sql(name='matching', con=engine, if_exists='replace',index=False)

conn.close()