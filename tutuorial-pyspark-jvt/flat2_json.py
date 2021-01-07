import json
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("jsonconversion").getOrCreate()
df = spark.read.csv("C:/Users/jvt/PYSPARKTRAINING/24septsolutions/file.txt",header=True,sep="|")
df_json = df.toJSON()        #JSON encoded string

result =[]

for row in df_json.collect():
    line = json.loads(row)
    result.append(line)

with open("C:/Users/india/Desktop/json_work/jsaonoutput.json",'w')as f:
        f.write(json.dumps(result,indent=4))




