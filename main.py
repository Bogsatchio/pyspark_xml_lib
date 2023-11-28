import os
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, size
from pyspark.sql.functions import first, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, StructType, StructField, BooleanType, LongType, IntegerType, DateType
from functools import reduce
from pyspark.sql.functions import explode
import json
from output_x_strings import output_x_strings

xml_52 = r"/user/bogam/Input/sftr/052_generated_sample.xml" #sample

spark = SparkSession \
        .builder \
        .appName("sftr") \
        .enableHiveSupport() \
        .getOrCreate()


df = spark.read.format("com.databricks.spark.xml").option("rowTag", "n1:Document").option("ignoreNamespace", True).load(xml_52)

xpaths = output_x_strings(df, "Document.SctiesFincgRptgTxRpt")


