from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
#data="s3://venudataset/us-500.csv"
data="s3://venudataset/bank-full.csv"
#data=sys.argv[1]
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
df.show()

