## This is our project implementation on Weather Sensor Data Preprocessing, Management and Anomaly Detection

Two types of databases are used for data management system: 
- Elastic Search
- Oracle RDMS

The preprocessing code for loading data into both databases is available in Preprocessing_Logstash.py and Preprocessing_MySQL.py files respectively. 

The anomaly detection algorithms employed currently are:
- Principal Component Analysis 
- Isolation Forest Algorithm


Data Distribution and Statistical analysis has been implemented to understand the various sensor parameters. The code is available in Outlier_AnomalyDetection.py file
