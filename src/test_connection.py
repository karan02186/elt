from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SQLServer_Test")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

jdbc_url = (
    "jdbc:sqlserver://192.168.68.62:1433;"
    "databaseName=BookingDB;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

connection_properties = {
    "user": "sa",
    "password": "India123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="booking.table_booking",
    properties=connection_properties
)

df.show()
df.printSchema()

spark.stop()