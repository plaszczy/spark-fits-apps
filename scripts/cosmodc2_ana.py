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


nside=2048
pixarea=hp.nside2pixarea(nside, degrees=True)*3600
reso= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

def densitymap(df,minmax=None):
    df_map=df.select("ipix").groupBy("ipix").count()
    df_map=df_map.withColumn("density",df_map['count']/pixarea).drop("count")

    #back to python world
    map_p=df_map.toPandas()
    A=map_p.index.size*pixarea/3600
    print("map area={} deg2".format(A))
    #statistics per pixel
    var='density'
    s=df_map.describe([var])
    s.show()
    r=s.select(var).take(3)
    N=int(r[0][0])
    mu=float(r[1][0])
    sig=float(r[2][0])
    map_p=df_map.toPandas()
    #now data is reduced create the healpy map
    skyMap= np.full(hp.nside2npix(nside),hp.UNSEEN)
    skyMap[map_p['ipix'].values]=map_p[var].values
    
    if minmax==None:
        minmax=(np.max([0,mu-2*sig]),mu+2*sig)
    hp.gnomview(skyMap,rot=[61.8,-35.2],reso=reso,min=minmax[0],max=minmax[1],title=r"$density/arcmin^2$")
    plt.show()



##################


import os,sys
sys.path.insert(0,"..")
from df_tools import *

###############
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
ff='/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet/*'
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
df_all.printSchema()
timer.step(ana)

ana="filter+cache"

df=df_all.select("halo_id","redshift",'ra','dec','mag_i').filter("halo_id>0")
#df=df_all.filter(df_all.halo_id>0).select('ra','dec','mag_u','mag_g','mag_r','mag_i','mag_z','mag_y')

df=df.cache()
print(df.count())

df=df.coalesce(50)

timer.step(ana)



ana="stat"
df.describe().show()
timer.step(ana)

ana="minmax redshift"
m=minmax(df,'redshift')
print("z \in [{},{}]".format(m[0],m[1]))
timer.step(ana)

ana="histo redshift"
h_z=df_histplot(df,'redshift',100)
timer.step(ana)


#ana="number of haloes"
#df_halo=df.groupBy("halo_id").count().cache()
#df_halo.describe(['count']).show()
#h_z=df_histplot(df_halo,'count',100)
#ana="join by halo_id count"
#df=df.join(df_halo,"halo_id").cache().withColumnRenamed("count","halo_members").cache()
d#f.count()
#gros cluster:
#df.filter(df.halo_members>300).groupby("halo_id").count().show()
