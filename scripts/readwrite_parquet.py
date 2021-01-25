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
import os,sys

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
        print("@"+ana+": {:2.1f} s".format(self.dt))
        print("-"*30)

        self.t0=t1
        return self.dt        


#input
fin=os.environ['SKYSIM']

fin="/global/cscratch1/sd/plaszczy/Skysim5000/skysim5000_v1.1.1_parquet"
print("*"*50)
print(fin)

fout="/global/cscratch1/sd/plaszczy/skysim5000_sub13.parquet"


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

timer=Timer()
ana="load"
df_all=spark.read.parquet(fin)
df_all.printSchema()
timer.print(ana)

ana="sub_extract"
#partitions
#decimate defualt nmber of parttions
decpart=10

numPart=df_all.rdd.getNumPartitions()
nrepart=numPart//decpart

gal=df_all.coalesce(nrepart)
print("defaut #parts={}, new one={}".format(numPart,nrepart))

#vars
cols=["galaxy_id","ra","dec","redshift","shear_1","shear_2"]
for b in ['u','g','r','i','z','y']:
    cols+=["mag_{}".format(b)]

gal=gal.select(cols)
#filter
gal=gal.filter(gal['mag_i']<25.3)

timer.print(ana)
print("#gal parts={}".format(gal.rdd.getNumPartitions()))

####

gal.write.option("compression","snappy").parquet(fout)

