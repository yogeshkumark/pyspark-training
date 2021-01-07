from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
appName = "PySpark Example - JSON file to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
schema = StructType([
    StructField('ID', StringType(), True),
    StructField('name', StringType(), True),
    StructField('DOB', StringType(), True),
    StructField('Gender', StringType(), True),
    StructField('Age', StringType(), True)
])

json_file_path = 'C:\\Users\india\Desktop\json_work\data.json'
df1 = spark.read.json(json_file_path, schema, multiLine=True)
# data = json.load(df1)
df_json = df1.toJSON()
for row in df1.rdd.collect():
    print(row.name,row.Age)



