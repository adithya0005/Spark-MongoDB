from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
from pymongo import MongoClient

header_list_climate = ["DataType", "Station", "Air Temperature(Celsius)", "Relative Humidity", "Wind Speed(Knots)", "Max Wind Speed", "MAX", "MIN", "Precipitation"]
header_list_fire = ["DataType", "Latitude", "Longitude", "Surface Temperature(Kelvin)", "Power", "Confidence", "Surface Temperature(Celsius)"]


def process_and_send_data(iter):
    client = MongoClient()

    db = client['taskC']
    collection = db['Weather']
    for each in iter:
        temp_dict_climate = {}
        temp_dict_fire = {}
        fire_list = []
        for i in range(len(header_list_climate)):
            temp_dict_climate[header_list_climate[i]] = each[1][0][i]
        for each_fire in each[1][1]:
            # print(str(each_fire))
            for i in range(len(header_list_fire)):
                temp_dict_fire[header_list_fire[i]] = each_fire[i]
            fire_list.append(temp_dict_fire)
        temp_dict_climate["fire"] = fire_list
        #print(str(each[1][0])+ "-" +str(each[1][1]))
        print(str(temp_dict_climate))
        try:
            collection.insert_many([temp_dict_climate])
        except:
            print("Exception Occurred")
    client.close()

# We add this line to avoid an error : "Cannot run multiple SparkContexts at once". If there is an existing
# spark context, we will reuse it instead of creating a new context.
sc = SparkContext.getOrCreate()

# If there is no existing spark context, we now create a new context
if sc is None:
    sc = SparkContext(master="local[2]", appName="Spark_data_Proc")
ssc = StreamingContext(sc, 5)

host = "localhost"
port = 9999

lines = ssc.socketTextStream(host, int(port))
window1 = lines.window(5)

firelines = ssc.socketTextStream(host, 9998)
window2 = firelines.window(5)

w1 = window1.map(lambda x: json.loads(x)).map(lambda y: y.get("climate")).map(lambda z: (z[0], z[1]))
w2 = window2.map(lambda y: json.loads(y)).map(lambda y: y.get("fire")).map(lambda z: (z[0], z[1]))

joined_rdd = w1.join(w2)
joined_rdd.foreachRDD(lambda func: func.foreachPartition(process_and_send_data))

ssc.start()
try:
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()

ssc.stop()
sc.stop()
