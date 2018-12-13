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
import matplotlib.pyplot as plt


import os,sys
sys.path.insert(0,"..")
from df_tools import *

from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
        self.dt=0.
        
    def step(self,ana):
        t1=time()
        self.dt=t1-self.t0
        self.t0=t1
        print(ana+": {:2.1f}s ".format(self.dt))
        return self.dt

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

ana="load"
ff=os.path.join(os.environ['COSMODC2'],"xyz_v1.1.4.parquet")
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
df_all.printSchema()
timer.step(ana)

ana="cache"
df=df_all.select("halo_id","ra","dec","redshift").filter("halo_id>0").cache()
print(df.count())
timer.step(ana)

ana="stat"
df.describe().show()
timer.step(ana)

ana="minmax redshift"
m=minmax(df,'redshift')
print("z \in [{},{}]".format(m[0],m[1]))
timer.step(ana)

ana="histo redshift"
h_z=plot_histo(df,'redshift',100)
timer.step(ana)


ana="number of haloes"
df_halo=df.groupBy("halo_id").count().cache()
#df_halo.describe(['count']).show()
plot_histo(df_halo,'count',100)

ana="join by halo_id count"
df=df.join(df_halo,"halo_id").cache().withColumnRenamed("count","halo_members").cache()
df.count()

df.show()
minmax(df.filter(df['halo_members']==1),'is_central')

#GUY
from pyspark.sql import functions as F

df=df_all.select("halo_id","ra","dec","redshift").filter("halo_id>0")
center_ra = 62
half_ra = 0.9
center_dec = -38.6
half_dec = 0.9

df=df.filter( (F.abs(df.ra-center_ra)<half_ra) & \
                  (F.abs(df.dec-center_dec)<half_dec) & \
                  (df.redshift.between(1,1.2)) )\
                  .drop('halo_id').cache()

df.count()
df.toPandas().to_csv('out.csv')
