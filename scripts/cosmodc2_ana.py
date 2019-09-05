#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import randn
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import healpy as hp
import matplotlib.pyplot as plt

####mystuff
import os,sys
sys.path.insert(0,"..")

from Timer import Timer
from df_tools import *
from qa_tools import *


#main
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

#
timer=Timer()
#
timer.start()
ff=os.environ['COSMODC2']
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
print("#partitions={}".format(df_all.rdd.getNumPartitions()))
df_all.printSchema()

ana="filter+cache"
#df=df_all.select("halo_id","redshift",'ra','dec','mag_i').filter("halo_id>0")

bands=['u','g','r','i','z','y']


#FILTER
df=df_all.filter(df_all.halo_id>0)

#SELECT COLS
cols="ra,dec,redshift"
for b in bands:
    s=",mag_{0},mag_true_{0}".format(b)
    cols+=s
print("selection=",cols)

#use these columns
df=df.select(cols.split(','))


print("output cols=> "+cols)

# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)

#CACHE
print("caching...")
df=df.cache()

print("size={} M".format(df.count()/1e6))

timer.stop()

## COALESX\CE?
df=df.coalesce(450)
print("coalesce N=".format(df.count))


