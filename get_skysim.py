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
ff=os.environ['SKYSIM']
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
print("def #partitions={}".format(df_all.rdd.getNumPartitions()))
df_all.printSchema()


#vars
cols=["ra","dec","redshift","mag_i"]

#SELECTION
gal=df_all.select(cols)

#FILTER
#gal=gal.filter(df_all['mag_i']<25.3)

print('add healpixels')
gal=add_healpixels(gal)
print("nside={}".format(nside))

#cache
#gal=gal.coalesce(670)
gal=gal.cache()

ana="count(cache)"
print("N={}G".format(gal.count()/1e9))

gal.printSchema()

timer.stop()

#centre
cen=[44.98588185317275,-32.70030646312413]
