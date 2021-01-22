#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import randn
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType


import pandas as pd
import numpy as np
import healpy as hp
import os

from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
        self.dt=0.

    def print(self,ana):
        t1=time()
        self.dt=t1-self.t0

        print("-"*30)
        print(ana+"& {:2.1f} &".format(self.dt))
        print("-"*30)

        self.t0=t1
        return self.dt        


#main
ff=os.environ['SKYSIM']
print(ff)

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)


timer=Timer()
ana="load"
df_all=spark.read.parquet(ff).coalesce(10000)
gal=df_all.select("ra","dec","redshift","mag_i").withColumnRenamed("redshift","z")
gal.printSchema()
timer.print(ana)
print("#parts={}".format(gal.rdd.getNumPartitions()))

####
ana="cache (count)"
gal=gal.cache()#.persist(StorageLevel.MEMORY_ONLY_SER)
print("N={}".format(gal.count()))
timer.print(ana)

#####
ana="stat z"
gal.describe(['z']).show()
timer.print(ana)

ana="stat all"
gal.describe().show()
timer.print(ana)

ana="minmax"
minmax=gal.select(F.min("z"),F.max("z")).first()
zmin=minmax[0]
zmax=minmax[1]
Nbins=100
dz=(zmax-zmin)/Nbins
timer.print(ana)

ana="histo (pUDF)"
@pandas_udf("float", PandasUDFType.SCALAR)
def binFloat(z):
    return pd.Series((z-zmin)/dz)
#dont know how to cast in pd so do it later
p_udf=gal.select(gal.z,binFloat("z").astype('int').alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
timer.print(ana)


###############

