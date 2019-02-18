#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import randn
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import os
import pandas as pd
import numpy as np
import healpy as hp

from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
        self.dt=0.

    def step(self):
        t1=time()
        self.dt=t1-self.t0
        self.t0=t1
        return self.dt

    def print(self,ana):
        print(ana+"& {:2.1f} &".format(self.dt))
        print("-"*30)


def benchmark(gal,z1,z2):
 
    return myMap

###############
nside=512
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
        return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

#main

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

timer=Timer()

#FITS
#gal=spark.read.format("fits").option("hdu",1)\
#     .load(os.environ['FITSDIR'])\
#     .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))

#PKT   
PARQUET="hdfs://134.158.75.222:8020/user/julien.peloton/LSST10Y_shuffled_uncomp"
gal=spark.read.parquet(PARQUET)\
	.select(F.col("RA"), F.col("DEC").alias("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))
      
gal.printSchema()
timer.step()
timer.print("load")
#######
gal=gal.withColumn("zrec",(gal.z+0.03*(1+gal.z)*randn()).astype('float'))
gal.show(5)
timer.step()
timer.print("show")
##cache
gal=gal.cache()
print("N={}".format(gal.count()))
timer.step()
timer.print("data loaded")
####

zshell=[0.0,0.13,0.27,0.43,0.63,0.82,1.05,1.32,1.61,1.95,2.32]
#zshell=[0.1,0.2,0.3,0.4,0.5]

#writemap
write=False
dt=[]

for z1,z2 in zip(zshell[0:-1],np.roll(zshell,-1)):
    #filter
    shell=gal.filter(gal['zrec'].between(z1,z2))
    print("shell=[{},{}] #={}".format(z1,z2,shell.count()))
    #add pixnumber
    shell=shell.withColumn("ipix",Ang2Pix("RA","Dec").alias("ipix"))
    #histogram
    p_map=shell.select("ipix").groupBy("ipix").count().toPandas()
    myMap = np.zeros(12 * nside**2)
    myMap[p_map['ipix'].values]=p_map['count'].values
    dt.append(timer.step())
    timer.print("shell=[{},{}]".format(z1,z2))
    if write:
        hp.write_map("map{}.fits".format(i), map)

ddt=np.array(dt)
with open("tomo_python.txt", 'ab') as abc:
    np.savetxt(abc, ddt.reshape(1,ddt.shape[0]))
    
