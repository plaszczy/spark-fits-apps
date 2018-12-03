#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import randn
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import os,sys

import pandas as pd
import numpy as np
import healpy as hp
import matplotlib.pyplot as plt

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
ff=os.path.join(os.environ['COSMODC2'],"xyz_v1.0.parquet")

df_all=spark.read.parquet(ff)
df_all.printSchema()
timer.step(ana)

ana="cache"

df=df_all.select('ra','dec','redshift','redshift_true').cache()

N=df.count()
print(N)
timer.step(ana)

ana="stat"
df.describe().show()
timer.step(ana)

ana="minmax redshift"
m=minmax(df,'redshift')
print("z \in [{},{}]".format(m[0],m[1]))
timer.step(ana)

ana="histo redshift"
h_z=df_histo(df,'redshift',100,bounds=(0,3)).toPandas()
dz=3./100
plt.bar(h_z['loc'].values,h_z['count'].values,dz,label='redshift',color='white',edgecolor='black')
plt.xlabel("z")
plt.ylabel("dN/dz")
plt.show()

timer.step(ana)

