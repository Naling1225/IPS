import pandas as pd
#import mariadb
import sys
import pymysql

from sqlalchemy import create_engine

#
# try:
#     conn = mariadb.connect(
#         user="admin",
#         password="ips12341234",
#         host= "ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com",
#         port=3306,
#         database="ips"
#     )
# except mariadb.Error as e:
#     print(f"Error connecting to MariaDB Platform: {e}")
#     sys.exit(1)


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

def str2df(new_user):
    elements = new_user.split(",")
    print(elements)
    user=pd.DataFrame([elements], columns=['id','life_style', 'relation', 'rest_style', 'share', 'shower', 'clean', 'guest'])
    return user

#시작

pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")

users= pd.read_sql("SELECT * FROM characteristic;",engine,index_col='id')
pref=pd.read_sql("SELECT * FROM likePer;",engine,index_col='char')
print(users)
# #users = pd.read_csv('users.csv', index_col='name')
char = []
# # print(classify(users.loc[users.index=='Jenny']))
print(users.index)
for name in users.index:
    char.append(classify(users.loc[users.index == name]))
print(sys.argv)
print(sys.argv[1])
new_user=sys.argv[1]
users['char'] = char
user=str2df(new_user)
print(users)

print(user)

print(classify(user))
prob = pref.loc[pref.index == classify(user)]
prob = prob.sort_values(by=classify(user), axis=1, ascending=False)
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
    probb.append(prob.at[classify(user),j])
db['prob']=probb

db=db.drop('char', axis='columns')
print(db)
success="success"
pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
db.to_sql(name='matching', con=engine, if_exists='replace', index=False)

#conn.close()