import os,sys
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import *
import pandas as pd
import numpy as np
import healpy as hp

import matplotlib.pyplot as plt
plt.set_cmap('jet')

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

nside=2048
area=hp.nside2pixarea(nside, degrees=True)*3600
resol= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))



def projmap(df):
    df_map=df.groupBy("ipix").count()
    df_map=df_map.withColumn("dens",df_map['count']/area).drop("count")
    #statistics per pixel
    df_map.describe(['dens']).show() 
    #df_histplot(df_map,"dens",doStat=True)
    #back to python world
    map_p=df_map.toPandas()
    A=map_p.index.size*area/3600
    print("map area={} deg2".format(A))
    #now data is reduced create the healpy map
    map_c = np.full(hp.nside2npix(nside),hp.UNSEEN)
    map_c[map_p['ipix'].values]=map_p['dens'].values
    return map_c



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
cols=["tract","patch","ra","dec","good","clean","extendedness","blendedness","mag_i_cModel","snr_i_cModel"]
print(cols)
#use these columns
df=df_all.select(cols)


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


dens_map=projmap(df)
A=np.sum(dens_map!=hp.UNSEEN)*area/3600
print("map area={} deg2".format(A))
#plot
hp.gnomview(dens_map,rot=[55,-29.8],reso=resol,min=100,max=400)
plt.show()
#plt.savefig("newrun.png")

####
#qual
dfqual=df.filter((df.good==True) & (df.clean==True) &(df.extendedness>0.9)) 
Nqual=dfqual.count()
print("dfqual N={} tot_frac={}".format(Nqual,Nqual/N))
print("#nans={}".format(num_nans(dfqual)))


print("qual stat:")
for c in df.columns:
    N_nans=Nqual-dfqual.select(c).na.drop().count()
    print("{}: NANs={}M ({:2.1f}%)".format(c,N_nans/1e6,float(N_nans/Nqual)*100))

#with i band information
dfqual_i=dfqual.select("ra","dec","mag_i_cModel","snr_i_cModel","ipix").na.drop()
Nqual_i=dfqual_i.count()
print("dfqual N={} tot_frac={}".format(Nqual_i,Nqual_i/Nqual))
print("#nans={}".format(num_nans(dfqual_i)))


#i<24
df24=dfqual_i.filter(dfqual_i['mag_i_cModel']<24)
df_map=df24.groupBy("ipix").count()
df_map=df_map.withColumn("dens",df_map['count']/area).drop("count")
print("density for i<24")
df_map.describe().show()


Nexp=40*10**(-0.36)
print("exp number={}/sq-arcmin tot={}".format(Nexp,Nexp*A*3600))
