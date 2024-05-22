# Structured_data_Streaming 

![image](https://github.com/00VALAK00/Structured_data_streaming/assets/117487025/ab1e40d0-f99e-43c3-91a8-01ddcaf2e46a)

## 1.About the project
The objective of this project is to Generate and Consume sensors data in real-time and then store it to the database for later processing and for this we need:
- Kafka : Event streaming platform (ESP): produce the messages 
- Spark’ s structured streaming API : consume the messages, comfy them to a schema and store it to db
- cassandra : (wide-column store optimized for data retrieval and writing)

## 2.Data pipeline 
- step1 : sensors are generated through an API and produced by kafka every 20s (temperature, humidity…)

- step2 : 
the json data is then consumed by spark and loaded it into a dataframe for processing

- step3 :
spark load the dataframe into a database after proper conection 
