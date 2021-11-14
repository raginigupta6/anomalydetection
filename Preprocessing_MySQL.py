from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
import pymongo
import json
from pymongo import MongoClient
import time
# connection = MongoClient("mongodb://10.184.51.194:27017")
connection = MongoClient("mongodb://10.184.61.202:27017")


# mongo_uri = "mongodb://iaf:" + quote("ragini123!@#") + "@10.184.61.202:27017"
# # connection = MongoClient('mongodb://iaf:cdac123!@#@10.184.51.194:27017')
# connection = MongoClient(mongo_uri, authSource="admin")
# db=connection['star']
db=connection['star20']
##SONIC
# 10 columns.
rotronics_not_enabled_columns = ['timestamp', 'sensor_id', 'u_component', 'v_component','w_component', 'units_id', 'speed_of_sound', 'sonic_temperature', 'error_code', 'checksum', 'date', 'datetime', 'time']

# 14 columns
rotronics_enabled_columns = ['timestamp', 'sensor_id', 'u_component', 'v_component','w_component', 'units_id', 'speed_of_sound', 'sonic_temperature', 'error_code', 'v1','v2', 'v3', 'v4', 'checksum','date', 'datetime', 'time']

##CR3000
#11 columns
metdata_columns = ['timestamp','table_name', 'battery_voltage', 'panel_temp', 'barometric_pressure', 'rainfall', 'air_temp', 'humidity', 'temperature_at10', 'temperature_at7','temperature_at4','date', 'datetime', 'time']

#15

solar_columns = ['timestamp','table_name', 'battery_voltage', 'shortwave_radiation_upward', 'shortwave_radiation_downward', 'longwave_radiation_upward', 'longwave_radiation_downward','sensor_temp_celsius', 'sensor_temp_kelvin', 'longwave_up_temp', 'longwave_down_temp', 'albedo', 'net_radiation', 'net_shortwave_radiation', 'rsnet','rlnet','date', 'datetime', 'time']

#5
datacold_columns = ['timestamp', 'table_name', 'battery_voltage','u_sen', 'u_heat','date', 'datetime', 'time']

#7
datawarm_columns = ['timestamp', 'table_name','u_sen', 'u_senamp', 'thermal_conductivity', 'e1', 'e1_q','date', 'datetime', 'time']

#4 columns
dynadata_columns = ['timestamp', 'table_name','thermal_diffusivity', 'volumetric_heat','date', 'datetime', 'time']

#4columns
mean_columns = ['timestamp', 'table_name','soil_heat','recalibrated_flux','date', 'datetime', 'time']

# 13 column
soil_columns = ['timestamp','name','year','julian_day','time','voltage1','voltage2','voltage3','voltage4','voltage5','soil_temp','soil_moisture','dielectric_Permittivity','date', 'datetime', 'time']

##CR6
#13 columns
logger_columns = ['timestamp', 'year', 'julian_day', 'time', 'battery_voltage', 'panel_temp', 'barometric_pressure', 'rainfall','temperature_at2','humidity_at2', 'temperature_at10', 'solar_radiation_watts', 'solar_radiation_kilowatts','date', 'datetime', 'time']

# 7 columns
logger7_columns = ['timestamp', 'year', 'julian_day', 'battery_voltage', 'panel_temp', 'barometric_pressure', 'rainfall','date', 'datetime', 'time']

#12 columns
soil_prob_columns = ['timestamp','name','year','julian_day','voltage1','voltage2','voltage3','voltage4','voltage5','soil_temp','soil_moisture','dielectric_Permittivity','date', 'datetime', 'time']
import pandas as pd
import glob

path = '/home/ragini/documents/jer43-2018-11 (1)/jer43/20181107/' # use your path
all_files = glob.glob(path + "/*.txt.gz")

##SONIC
rotronics_enabled_df = pd.DataFrame()
rotronics_not_enabled_df = pd.DataFrame()

##CR300
metdata_df = pd.DataFrame()
solar_df = pd.DataFrame()
datacold_df = pd.DataFrame()
datawarm_df = pd.DataFrame()
dynadata_df = pd.DataFrame()
mean_df = pd.DataFrame()
soil_df =  pd.DataFrame()

##CR6
logger_df =pd.DataFrame()
logger7_df = pd.DataFrame()
soil_prob_df = pd.DataFrame()

for filename in all_files[0:5]:
    ## file name spilt to add extra column to the table
    date, time  = filename.split('/')[-1].split('_')[0:2]
#     print(date, time)

    #YYYY-MM-DD HH:mm:ss as - Grafana date time format
    date = date[:4]+"-"+ date[4:6]+"-"+date[6:]
    time = time[:2]+":"+time[2:4]
    datetime = date+" "+time
    
    ##read each file into Dataframe 
    df = pd.read_csv(filename, index_col=None, header=None, skiprows=1)    
    print("File name:: ", filename, " total Column :", df.shape)

    
    ##SONIC LOG    
    if(df.shape[1] == 10):
        rotronics_not_enabled_df=pd.concat([rotronics_not_enabled_df,df],axis=0, ignore_index=True)
        print("File name:: ", filename, " total Column :", df.shape)
        rotronics_not_enabled_df['date'] = date
        rotronics_not_enabled_df['time'] = time 
        rotronics_not_enabled_df['datetimetime'] = datetime 
        
    elif(df.shape[1] == 14):
        rotronics_enabled_df= pd.concat([rotronics_enabled_df,df],axis=0, ignore_index=True)
        rotronics_enabled_df['temperature'] = [(v-2)*20 for v in rotronics_enabled_df['v3']]
        rotronics_enabled_df['humidity'] = [v*100 for v in rotronics_enabled_df['v4']]        
        rotronics_enabled_df['date'] = date
        rotronics_enabled_df['time'] = time 
        rotronics_enabled_df['datetimetime'] = datetime
        
        
    ##CR3000 log
    elif(df.shape[1] == 11):
        metdata_df= pd.concat([metdata_df,df],axis=0, ignore_index=True)
        metdata_df['date'] = date
        metdata_df['time'] = time 
        metdata_df['datetimetime'] = datetime
        
    elif(df.shape[1] == 15):
        solar_df= pd.concat([solar_df,df],axis=0, ignore_index=True)
        solar_df['date'] = date
        solar_df['time'] = time 
        solar_df['datetimetime'] = datetime
        
    elif(df.shape[1] == 5):
        datacold_df= pd.concat([datacold_df,df],axis=0, ignore_index=True)
        datacold_df['date'] = date
        datacold_df['time'] = time 
        datacold_df['datetimetime'] = datetime
    
    elif(df.shape[1] == 7):
        if(df.iloc[1, 1]) == 'DataWarm':           
            datawarm_df= pd.concat([datawarm_df,df],axis=0, ignore_index=True)
            datawarm_df['date'] = date
            datawarm_df['time'] = time 
            datawarm_df['datetimetime'] = datetime
        else:
            logger7_df= pd.concat([logger7_df,df],axis=0, ignore_index=True)
            logger7_df['date'] = date
            logger7_df['time'] = time 
            logger7_df['datetimetime'] = datetime
            
    elif(df.shape[1] == 4):
        if(df.iloc[1, 1]) == 'DynaData':           
            dynadata_df= pd.concat([dynadata_df,df],axis=0, ignore_index=True)
            dynadata_df['date'] = date
            dynadata_df['time'] = time 
            dynadata_df['datetimetime'] = datetime
        elif(df.iloc[1, 1]) == 'Mean':           
            mean_df= pd.concat([mean_df,df],axis=0, ignore_index=True)
            mean_df['date'] = date
            mean_df['time'] = time 
            mean_df['datetimetime'] = datetime
             
    elif(df.shape[1] == 13):
        if('Prob' in df.iloc[1, 1]):           
            soil_df= pd.concat([soil_df,df],axis=0, ignore_index=True)
            soil_df['date'] = date
            soil_df['time'] = time 
            soil_df['datetimetime'] = datetime             
        else:           
            logger_df= pd.concat([logger_df,df],axis=0, ignore_index=True)
            logger_df['date'] = date
            logger_df['time'] = time 
            logger_df['datetimetime'] = datetime
             
    elif(df.shape[1] == 12):
        soil_prob_df= pd.concat([soil_prob_df,df],axis=0, ignore_index=True)
        soil_prob_df['date'] = date
        soil_prob_df['time'] = time 
        soil_prob_df['datetimetime'] = datetime
    else:
        print("NO MATCHING :: ", filename, " SIZE :: ", df.shape)
             
        
             
    
if(rotronics_enabled_df.shape[0] > 0):
    rotronics_enabled_df.columns = rotronics_enabled_columns

if(rotronics_not_enabled_df.shape[0] > 0):
    rotronics_not_enabled_df.columns = rotronics_not_enabled_columns
    
if(metdata_df.shape[0] > 0):
    metdata_df.columns = metdata_columns
    
if(solar_df.shape[0] > 0):
    solar_df.columns = solar_columns
             
         
if(datacold_df.shape[0] > 0):
    datacold_df.columns = datacold_columns
             
if(datawarm_df.shape[0] > 0):
    datawarm_df.columns = datawarm_columns


if(dynadata_df.shape[0] > 0):
    dynadata_df.columns = dynadata_columns


if(mean_df.shape[0] > 0):
    mean_df.columns = mean_columns
             
  
if(soil_df.shape[0] > 0):
    soil_df.columns = soil_columns


if(logger_df.shape[0] > 0):
    logger_df.columns = logger_columns


if(logger7_df.shape[0] > 0):
    logger7_df.columns = logger7_columns
           
if(soil_prob_df.shape[0] > 0):
    soil_prob_df.columns = soil_prob_columns
    
    # record1 = db['qpattern']
# # ##drop
# db.anomaly.drop()
# # ##Insert
records = json.loads(ans_df.T.to_json()).values()
db.anomaly.insert_many(records)
print("ANOMALY DETECTION : DONE")
print("time to complete function : ", time.time() - stime)
# for filename in all_files[0:5]:
#     df = pd.read_csv(filename, index_col=None, header=None, skiprows=1)    
#     print("2nd column", df.iloc[1,1]=='Q')
#     ##SONIC LOG
# #     filename.split('/')[-1].split('_')[0:3]
#     date, time  = filename.split('/')[-1].split('_')[0:2]
#     print(date, time)
#     #YYYY-MM-DD HH:mm:ss as
#     date = date[:4]+"-"+ date[4:6]+"-"+date[6:]
#     time = time[:2]+":"+time[2:4]
#     datetime = date+" "+time
    
#     datetime
    
#     rotronics_not_enabled_df['date'] = date[:-2]

rotronics_not_enabled_df.shape
rotronics_not_enabled_df.head(2)
soil_df.shape
logger_df.shape
logger_df.info()

from sqlalchemy import create_engine
# !pip install sqlalchemy
# !pip install mysql-connector-python

import mysql.connector


## First create a database in MySQL called logdata and create two tables there for logger and sonic. Set userID and password to root and root on MySQL
# Set database credentials.
creds = {'usr': 'root',
             'pwd': 'root',
             'hst': '127.0.0.1',
             'prt': 3306,
             'dbn': 'logdata'}
             
connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
    # Create sqlalchemy engine for MySQL connection.
engine = create_engine(connstr.format(**creds))
## writing data from logger data frame to logger database table in mysql
logger_df.to_sql(name='logger', con=engine, if_exists='append', index=False)
