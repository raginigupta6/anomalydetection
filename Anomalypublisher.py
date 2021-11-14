import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math

from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import *

from KafkaUtils import *
def centroid(data):
    return np.mean(data, axis = 0)


def anomaly_score(data, c, noise = 0.01):
    dist = {i: euclidean(data[i], c) for i in range(len(data))}

    minv = min(dist.values())
    maxv = max(dist.values())

    dist = {i: (dist[i] - minv + noise) / (maxv - minv + noise) for i in dist.keys()}

    return dist


def forest_temp(D):
    isolation_forest = IsolationForest(n_estimators=100)
    isolation_forest.fit(D['Temp2'].values.reshape(-1, 1))

    xx = np.linspace(D['Temp2'].min(), D['Temp2'].max(), len(D)).reshape(-1, 1)
    print(xx)

    anomaly_score = isolation_forest.decision_function(xx)
    print (anomaly_score)

    outlier = isolation_forest.predict(xx)
    print(len(outlier))
    return anomaly_score
def forest_humidity(D):
    isolation_forest = IsolationForest(n_estimators=100)
    isolation_forest.fit(D['Humidity'].values.reshape(-1, 1))

    xx = np.linspace(D['Humidity'].min(), D['Humidity'].max(), len(D)).reshape(-1, 1)
    print(xx)

    anomaly_score = isolation_forest.decision_function(xx)
    print (anomaly_score)

    outlier = isolation_forest.predict(xx)
    print(len(outlier))
    return anomaly_score


def forest_pressure(D):
    isolation_forest = IsolationForest(n_estimators=100)
    isolation_forest.fit(D['BP'].values.reshape(-1, 1))

    xx = np.linspace(D['BP'].min(), D['BP'].max(), len(D)).reshape(-1, 1)
    print(xx)

    anomaly_score = isolation_forest.decision_function(xx)
    print (anomaly_score)

    outlier = isolation_forest.predict(xx)
    print(len(outlier))
    return anomaly_score

def pca(Data):

    pca = PCA(n_components=2)
    Data = StandardScaler().fit_transform(Data)
    pca.fit(Data)
    Data = pca.transform(Data)
    return Data


D = pd.read_excel('data4_ragini.xlsx')

Data = []
for i in range(len(D)):
    point = list(D.loc[i, ['BP']]) + list(D.loc[i, ['Temp2']]) + list(D.loc[i, ['Humidity']])
    # print (point)
    Data.append(point)

Data = pca(Data)
c = centroid(Data)
score = anomaly_score(Data, c, noise = 0.01)
print (score)

Dict = {}
for i in range(len(D)):
    Dict[i] = list(D.loc[i, ['Time']]) + list(D.loc[i, ['BP']]) + list(D.loc[i, ['Temp2']]) + list(D.loc[i, ['Humidity']]) + [score[i]]
print (Dict)

#####################################################################################
anomaly_score_isa_temp = forest_temp(D)

Dict_isa_temp = {}
for i in range(len(D)):
    Dict_isa_temp[i] = list(D.loc[i, ['Time']]) + list(D.loc[i, ['Temp2']]) + [anomaly_score_isa_temp[i]]

print ("Temperature Anomalies with ISA",Dict_isa_temp)


anomaly_score_isa_humidity= forest_humidity(D)
Dict_isa_humidity = {}
for i in range(len(D)):
    Dict_isa_humidity[i] = list(D.loc[i, ['Time']]) + list(D.loc[i, ['Humidity']]) + [anomaly_score_isa_humidity[i]]

print ("Humidity Anomalies with ISA",Dict_isa_humidity)

###########################################################################################
anomaly_score_isa_pressure= forest_pressure(D)
Dict_isa_pressure= {}
for i in range(len(D)):
    Dict_isa_pressure[i] = list(D.loc[i, ['Time']]) + list(D.loc[i, ['BP']]) + [anomaly_score_isa_pressure[i]]

print ("Pressure Anomalies with ISA",Dict_isa_pressure)

##########################################################################################



# Address where the broker is running
broker = "192.17.102.230"
port = "9092"

#assumes a kafka broker is running on a specified port
pubsub = KafkaUtils(broker = broker, port = port)
print(pubsub.seeTopics())
topic_name_isa="Anomaly-IsolationForest"
pubsub.createTopic(topic_name_isa)
topic_name_pca="Anomaly-PCA"
pubsub.createTopic(topic_name_pca)

i=0

pubsub.sendData(topic_name_isa,Dict_isa_temp)
pubsub.sendData(topic_name_isa,Dict_isa_humidity)
pubsub.sendData(topic_name_isa,Dict_isa_pressure)
pubsub.sendData(topic_name_pca,Dict)
