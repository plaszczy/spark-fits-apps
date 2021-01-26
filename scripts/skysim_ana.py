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

sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import df_hist

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


#main
decpart=int(sys.argv[1])

#shared
#ff=os.environ['SKYSIM']

#cscratch
ff="/global/cscratch1/sd/plaszczy/Skysim5000/skysim5000_v1.1.1_parquet"
#ff="/global/cscratch1/sd/plaszczy/Skysim5000/skysim5000_gold13.parquet"
print("*"*50)
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
df_all=spark.read.parquet(ff)

#partitions
numPart=df_all.rdd.getNumPartitions()
nrepart=numPart//decpart
gal=df_all.coalesce(nrepart)
print("defaut #parts={}, new one={}".format(numPart,nrepart))

#vars
cols=["galaxy_id","ra","dec","redshift","shear_1","shear_2"]
for b in ['u','g','r','i','z','y']:
    cols+=["mag_{}".format(b)]


#SELECTION
gal=gal.select(cols)
#gal=gal.select("ra","dec","redshift","mag_i")
#FILTER
gal=gal.filter(df_all['mag_i']<25.3)


gal.printSchema()
timer.print(ana)
print("#gal parts={}".format(gal.rdd.getNumPartitions()))

gal=gal.cache()
####

ana="count(cache)"
print("N={}G".format(gal.count()/1e9))
timer.print(ana)

#reduce parts
nodes=int(os.environ['SLURM_JOB_NUM_NODES'])-1
ncores=nodes*32
npart=ncores*3
gal=gal.coalesce(npart)
print("#gal comput parts={}".format(gal.rdd.getNumPartitions()))


ana="minmax"
minmax=gal.select(F.min("redshift"),F.max("redshift")).first()
zmin=minmax[0]
zmax=minmax[1]
Nbins=100
dz=(zmax-zmin)/Nbins
timer.print(ana)


#####
ana="stat redshift"
gal.describe(['redshift']).show()
timer.print(ana)

ana="stat all"
gal.describe().show()
timer.print(ana)

ana="df_hist"
p=df_hist(gal,"redshift")
print(p)
timer.print(ana)


ana="histo (pUDF)"
@pandas_udf("float", PandasUDFType.SCALAR)
def binFloat(z):
    return pd.Series((z-zmin)/dz)
#dont know how to cast in pd so do it later
p_udf=gal.select(gal.redshift,binFloat("redshift").astype('int').alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
timer.print(ana)


###############

