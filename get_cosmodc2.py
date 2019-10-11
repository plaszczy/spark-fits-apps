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
#df=df_all.filter(df_all.halo_id>0)
df=df_all


#SELECTION
cols="halo_id,ra,dec,redshift"
#bands=['u','g','r','i','z','y']
bands=['i']
for b in bands:
    s=",mag_{0},Mag_true_{0}_lsst_z0".format(b)
    cols+=s
#use these columns
df=df.select(cols.split(','))

# renommage
for b in bands:
    df=df.withColumnRenamed("Mag_true_{}_lsst_z0".format(b),"M{}".format(b))

colbands=['b','g','r','y','m','k']

# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)

# nouvelles vars
#df=df.withColumn("g-r",df.mag_g-df.mag_r)
#df=df.withColumn("m-M",df.mag_r-df.Mr)
#df=df.withColumn("log10z",F.log10(df.redshift))
#df=df.withColumn("log10(Mstar)",F.log10(df.stellar_mass)).drop('stellar_mass')

print("After selection=")
df.printSchema()
print("#VARIABLES={} out of {} ({:3.1f}%)".format(len(df.columns),len(df_all.columns),float(len(df.columns))/len(df_all.columns)*100))

#re-filter
#df=df.sample(0.01)

#CACHE
#print("caching...")
#df=df.cache()

gold_true=df.filter(df.mag_i<25.3).cache()

print("size={} M".format(gold_true.count()/1e6))



timer.stop()

#cosmodc2
#rot=[61.81579482165925,-35.20157446022967]
