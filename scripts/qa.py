import os,sys
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import *

from pyspark.sql import SparkSession

# Initialise our Spark session
spark = SparkSession.builder.getOrCreate()
print("spark session started")

#usefull tool to benchmark
from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
    
    def start(self):
        self.t0=time()
        
    def stop(self):
        t1=time()
        print("{:2.1f}s".format(t1-self.t0))

timer=Timer()


# input
timer.start()
df_all=spark.read.parquet("/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog/dpdd_object_run1.2p.parquet")

df_all.printSchema()
timer.stop()


# build selection by appending to string
cols=["tract","patch","ra","dec","good","clean","extendedness","blendedness","psFlux_flag_i","psFlux_i","mag_i","mag_i_cModel","snr_i_cModel"]
print(cols)
#use these columns
df=df_all.select(cols)


# Apply some quality cuts
#df=df.filter( (df.good==True)&(df.clean==True)&(df.extendedness>0.9)&(df.blendedness < 10**(-0.375))&(df.mag_i_cModel< 24.5)&(df.snr_i_cModel>10))


# Add a column of healpixels (mapReduce way)
import pandas as pd
import numpy as np
import healpy as hp
from pyspark.sql.functions import pandas_udf, PandasUDFType

nside=2048
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

#add a column of healpix indices
df=df.withColumn("ipix",Ang2Pix("ra","dec"))

#
print("caching...")
timer.start()
df=df.cache()
N=df.count()
print("raw {}M objects".format(N/1e6))
timer.stop()


#attention au nans

#tract/patchs
print("#tracts={}".format(df.select("tract").distinct().count())) #check that all the tracts are present
df.groupBy(["tract","patch"]).count().groupBy("tract").count().\
withColumnRenamed("count","#patches").sort("tract").show() 

#stat nans
print("NaNs stat:")
for c in df.columns:
    N_nans=N-df.select(c).na.drop().count()
    print("{} : {:2.1f}% Nans".format(c,float(N_nans/N)*100))

vals=["good","clean","extendedness"]
for v in vals:
      df.select(v).groupby(v).count().show(5)


timer.start()
#groupby indices and count the number of elements in each group
df_map=df.groupBy("ipix").count()
area=hp.nside2pixarea(nside, degrees=True)*3600
df_map=df_map.withColumn("dens",df_map['count']/area).drop("count")
#statistics per pixel
df_map.describe(['dens']).show() 
#back to python world
map_p=df_map.toPandas()
#now data is reduced create the healpy map
map_c = np.full(hp.nside2npix(nside),hp.UNSEEN)
map_c[map_p['ipix'].values]=map_p['dens'].values
#map_c[map_c==0]=hp.UNSEEN
timer.stop()

A=map_p.index.size*area/3600
print("map area={} deg2".format(map_p.index.size*area/3600))
#plot

import matplotlib.pyplot as plt
plt.set_cmap('jet')
resol= hp.nside2resol(nside,arcmin=True)
hp.gnomview(map_c,rot=[55,-29.8],reso=resol,xsize=200,min=200,max=300)
plt.savefig("newrun.png")


#qual
dfqual=df.filter((df.mag_i_cModel<24) & (df.good==True) & (df.extendedness>0.9)) 
print("df qual i<24 N={}".format(dfqual.count()))
print("df qual i<24 SNR>10 N={}".format( dfqual.filter(df.snr_i_cModel>10).count()))
Nexp=40*10**(-0.36)*A*3600
print("exp number=".format(Nexp))

