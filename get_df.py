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

#FILTER
df=df_all.filter(df_all.halo_id>0)

#SELECTION
cols="halo_id,ra,dec,redshift,position_x,position_y,position_z,size_true,stellar_mass"
#bands=['u','g','r','i','z','y']
#for b in bands:
#    s=",mag_{0}".format(b)
#    cols+=s
#use these columns
df=df.select(cols.split(','))


print("After selection=")
df.printSchema()
print("#VARIABLES={} out of {} ({:3.1f}%)".format(len(df.columns),len(df_all.columns),float(len(df.columns))/len(df_all.columns)*100))

# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)


#fileter
#df=df.filter(df.ipix==38188148)

#CACHE
print("caching...")
df=df.cache()

print("size={} M".format(df.count()/1e6))

timer.stop()
