import pandas as pd
from pandas import DataFrame
import pymysql
from sqlalchemy import create_engine


file= 'C:/Users/user/PycharmProjects/IPS/ipsdata.csv'
df = pd.read_csv(file, encoding = "euc-kr")
print(df)


df.rename(columns={"Unnamed: 0":"num"},inplace=True)
#df.set_index('num', inplace=True)
df=df.astype('category')
print(df)
df.info()
rout_trend=pd.crosstab(index=df['M_rout'],columns=df['H_rout'],margins=True, normalize="index")
rltsh_trend=pd.crosstab(index=df['M_rltsh'],columns=df['H_rltsh'],margins=True,normalize="index")
FT_trend=pd.crosstab(index=df['M_FT'],columns=df['H_FT'],margins=True,normalize="index")
shower_trend=pd.crosstab(index=df['M_shower'],columns=df['H_shower'],margins=True, normalize="index")
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
pymysql.install_as_MySQLdb()
engine = create_engine("mysql://admin:ips12341234@ips.csvcpbtn5bj6.ap-northeast-2.rds.amazonaws.com:3306/ips")
db.to_sql(name='likePer', con=engine, if_exists='replace', index=False)

