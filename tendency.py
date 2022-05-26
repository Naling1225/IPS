import sys

import pandas as pd
from pandas import DataFrame
import pymysql
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
# df= pd.read_sql("SELECT * FROM good_mate;",engine,index_col='num')
#
# print(df)
# num=[]
# for i in range(len(df)):
#     num.append(int(i))
# ind=pd.DataFrame(columns=['num'], data=num)
# # preference=DataFrame(columns=['DFH','DFO','DSH','DSO','NFH','NFO','NSH','NSO'], data=arr2)
# good_mate=pd.concat([ind, df], axis = 1)
# print(df)
# pymysql.install_as_MySQLdb()
# engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
# # df.to_sql(name='good_mate', con=engine, if_exists='replace', index=False)

good_mate= pd.read_sql("SELECT * FROM good_mate;",engine,index_col='num')

# df.rename(columns={"Unnamed: 0":"num"},inplace=True)
#df.set_index('num', inplace=True)
# group=['Jenny','Winter']
group=sys.argv[1].split(",")
userId1=group[0]
userId2=group[1]

q="SELECT life_style, relation, rest_style FROM characteristic WHERE id='"
user1Char= pd.read_sql(q+userId1+"';",engine)
user2Char= pd.read_sql(q+userId2+"';",engine)
user1Char.rename(columns={'life_style':'M_rout','relation':'M_rltsh','rest_style':'M_FT'},inplace=True)
user2Char.rename(columns={'life_style':'H_rout','relation':'H_rltsh','rest_style':'H_FT'},inplace=True)
usersChar=pd.DataFrame(columns=['num'], data=[int(len(good_mate))])
print(user1Char)
print(user2Char)
usersChar=pd.concat([usersChar,user1Char,user2Char],axis=1)
print(usersChar)
usersChar.to_sql(name='good_mate', con=engine, if_exists='append', index=False)

good_mate= pd.read_sql("SELECT * FROM good_mate;",engine,index_col='num')

df=good_mate.astype('category')
print(df)
df.info()
rout_trend=pd.crosstab(index=df['M_rout'],columns=df['H_rout'],margins=True, normalize="index")
rltsh_trend=pd.crosstab(index=df['M_rltsh'],columns=df['H_rltsh'],margins=True,normalize="index")
FT_trend=pd.crosstab(index=df['M_FT'],columns=df['H_FT'],margins=True,normalize="index")

# shower_trend=pd.crosstab(index=df['M_shower'],columns=df['H_shower'],margins=True, normalize="index")
print(rout_trend)
print(rltsh_trend)
print(FT_trend)
#print(shower_trend)
arr = [[0 for j in range(4)] for i in range(4)]

for m in rltsh_trend:
    row=0
    for n in rltsh_trend:
        for i in FT_trend:
            j =  0
            if(m=='solo'):
                col=2
            else: col=0
            for j in FT_trend:
                arr[row][col] = rltsh_trend[m][n]*FT_trend[j][i]
                col = col + 1
            row = row +1
    col=col+1

arr2 = [[0 for j in range(8)] for i in range(8)]

for m in rout_trend:
    row=0
    for n in rout_trend:
        for i in range(4):
            j =  0
            if(m=='night'):
                col=4
            else: col=0
            for j in range(4):
                arr2[row][col] = rout_trend[m][n]*arr[i][j]
                col = col + 1
            row = row +1
    col=col+1

ind=pd.DataFrame(columns=['char'],data=['DFH','DFO','DSH','DSO','NFH','NFO','NSH','NSO'])
preference=DataFrame(columns=['DFH','DFO','DSH','DSO','NFH','NFO','NSH','NSO'], data=arr2)
db=pd.concat([ind, preference], axis = 1)
print(db)
#preference.to_csv("C:/Users/user/PycharmProjects/IPS/preference.csv",sep=',',index=True)
# pymysql.install_as_MySQLdb()
# engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
db.to_sql(name='likePer', con=engine, if_exists='replace', index=False)

