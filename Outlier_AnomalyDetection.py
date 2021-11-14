import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
#from pyod.models.copod import COPOD
D = pd.read_excel('D:\data4.xlsx')


D['Temp2'].describe()
# print (D)


sns.distplot(D['Temp2'])
plt.title("Distribution of Temperature at 2 meters altitude")
sns.despine()


## Skewness for Humidity


sns.distplot(D['Humidity'])
plt.title("Distribution of Humidity")
ns.despine()
print("Skewness: %f" % D['Humidity'].skew(skipna=True))
print("Kurtosis: %f" % D['Humidity'].kurt(skipna=True))


## Skewness for Temperature
sns.distplot(D['Temp2'])
plt.title("Distribution of Temperature")
sns.despine()
print("Skewness: %f" % D['Temp2'].skew(skipna=True))
print("Kurtosis: %f" % D['Temp2'].kurt(skipna=True))

## Skewness for AP
sns.distplot(D['BP'])
plt.title("Distribution of Air Pressure")
sns.despine()


print("Skewness: %f" % D['BP'].skew(skipna=True))
print("Kurtosis: %f" % D['BP'].kurt(skipna=True))


isolation_forest = IsolationForest(n_estimators=100)
isolation_forest.fit(D['BP'].values.reshape(-1, 1))
xx = np.linspace(D['BP'].min(), D['BP'].max(), len(D)).reshape(-1,1)
anomaly_score = isolation_forest.decision_function(xx)
outlier = isolation_forest.predict(xx)
plt.figure(figsize=(10,4))
plt.plot(xx, anomaly_score, label='anomaly score')
plt.fill_between(xx.T[0], np.min(anomaly_score), np.max(anomaly_score), 
                 where=outlier==-1, color='r', 
                 alpha=.4, label='outlier region')
plt.legend()
plt.ylabel('anomaly score')
plt.xlabel('Air Pressure')
plt.show();

## For PCA:

mynumpy=D.values
mynumpy=mynumpy[:,[6,8,9]]
mynumpy = StandardScaler().fit_transform(mynumpy)

pca = PCA(n_components=2)

#pca.fit(mynumpy)


mytable=pca.fit_transform(mynumpy)
print(pca.explained_variance_ratio_)
labels=[i for i in range(np.shape(mynumpy)[0])]
fig2, ax = plt.subplots()

for i in range(np.shape(mynumpy)[0]):
    plt.scatter(mytable[i,0],mytable[i,1])

for i, txt in enumerate(mytable):
    ax.annotate(labels[i], (mytable[i,0], mytable[i,1]))
    

plt.xlabel("PC-1")
plt.ylabel("PC-2")
plt.show()

feature1 = []
feature2 = []
time = []

fig, ax1 = plt.subplots()

for i in range(1, len(D)):
    l = list(D.loc[i])

    flag = 0
    for item in l:
        if item == 'nan' or item == 'NAN':
            flag = 1
            break

    if flag == 1:
        continue

    #print (l[8], l[9])

    #time.append(l[0])
    ## Converting time from seconds to minutes to hours:
    hr=int(l[0]/60)
    if hr<60:
        total_time="4:"+str(hr)
    elif hr>60 and hr<120:
        total_time="5:"+str((hr-60))
    time.append((l[0]/60))
    feature1.append(l[6])
    feature2.append(l[9])



ax2 = ax1.twinx()
ax1.plot(time, feature1, 'g-')
ax2.plot(time, feature2, 'b-')

ax1.set_xlabel('Time (Number of minutes past 4:00 PM)')
ax1.set_ylabel('Atmospheric Pressure(Millibar)', color='g')
ax2.set_ylabel('Humidity (%)', color='b')

plt.savefig('Datas.png')
plt.show()

