
from KafkaUtils import *

# Address where the broker is running
broker = "192.17.102.230"
port = "9092"

#assumes a kafka broker is running on a specified port
pubsub = KafkaUtils(broker = broker, port = port)
print(pubsub.seeTopics())


topic_name_isa="Anomaly-IsolationForest"
data_isa = pubsub.receiveData(topic_name=topic_name_isa)
if data_isa != None:
    print('Format: [timestamp,dimension,anomaly score]:', data_isa)

else:
    print('No new data available on topic: ', topic_name_isa)
topic_name_pca="Anomaly-PCA"
data_pca=pubsub.receiveData(topic_name=topic_name_pca)

if data_pca != None:
    print('Format: [timestamp, dim-1,dim-2,dim-3,anomaly score]', data_pca)

else:
    print('No new data available on topic: ', topic_name_pca)
