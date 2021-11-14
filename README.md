Requirements:
-numpy
-pandas
-scipy
-kafka-python
-kafkaUtils


1. Three topics are created by the Publisher process to execute two anomaly detection algorithms: Isolation Forest and PCA. 
2. The publisher sends anomaly results based on the timestamp and data points that are identified as anomalies. 
3. The subscriber subscribes to a topic of interest.
4. The publisher and subscriber should be running as two independent processes on terminal.
