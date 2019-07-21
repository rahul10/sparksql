import sys,re,commands
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import *
import pandas as pd
#import org.apache.spark.SparkConf
#import org.apache.spark.sql._
#import org.apache.spark.sql.SQLContext._
#import org.apache.spark.sql.hive.HiveContext


# We have to set the hive metastore uri.
#SparkContext.setSystemProperty("hive.metastore.uris", "hdfs://192.168.122.22:11111/spark-warehouse")

# Spark Session and DataFrame creation
spark = (SparkSession.builder
                .appName('hive')
                #.config("spark.sql.warehouse.dir", "file:///root/var/www/cgi-bin/hiveql/spark-warehouse")
                .enableHiveSupport()
                .master("local")	
                .getOrCreate())
                
tab=sys.argv[1]
#"spark.sql.warehouse.dir"

#data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
#df = sparkSession.createDataFrame(data)

# Write into Hive
#df.write.saveAsTable('example')

# Read from Hive																																																																														
result = spark.sql('desc '+tab+'') 
result.coalesce(1).write.format("csv").option("header","true").csv("m.csv")
p=commands.getoutput("ls /var/www/cgi-bin/hiveql/m.csv/ | grep csv")
pd.options.display.max_rows=30
pd.options.display.max_columns=16
pd.set_option('max_colwidth', 13)
pd.options.display.width=250
data = pd.read_csv("file:///var/www/cgi-bin/hiveql/m.csv/"+str(p)+"")
x="<html><h3>Table Structure</h3><br><pre>"+str(data)+"</pre><hr><br></html>"
print x
commands.getoutput("rm -rf /var/www/cgi-bin/hiveql/m.csv")
data2=pd.read_csv("file:///var/www/cgi-bin/hiveql/csv/"+tab+".csv")
x="<html><h3>Table Data</h3><br><pre>"+str(data2)+"</pre><hr></html>"
print x

#print p
#df_load.show()


