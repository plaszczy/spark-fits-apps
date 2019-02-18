from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

import os,sys
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import *

import pandas as pd
import numpy as np
import healpy as hp
import matplotlib.pyplot as plt
plt.set_cmap('jet')

dc2_run1x_center=[55.064,-29.783]
dc2_run1x_region = [[57.87, -27.25], [52.25, -27.25], [52.11, -32.25], [58.02, -32.25]]

dc2_udf_center=[53.125,-28.100]
dc2_udf_region=[[53.764,-27.533],[52.486,-27.533],[52.479,-28.667],[53.771,-28.667]]



nside=2048
pixarea=hp.nside2pixarea(nside, degrees=True)*3600
reso= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))


def projmap(df):
    df_map=df.groupBy("ipix").count()
    df_map=df_map.withColumn("dens",df_map['count']/pixarea).drop("count")
    #statistics per pixel
    df_map.describe(['dens']).show() 
    #df_histplot(df_map,"dens",doStat=True)
    #back to python world
    map_p=df_map.toPandas()
    A=map_p.index.size*pixarea/3600
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
ff="/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_v4/dpdd_dc2_object_run1.2p.parquet"
print("reading {}".format(ff))
timer.start()
df_all=spark.read.parquet(ff)
#df_all.printSchema()
timer.stop()

# build selection by appending to string
cols=["tract","patch","ra","dec","good","clean","extendedness","blendedness","mag_i_cModel"]
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
print("#raw objects ={}M".format(N/1e6))
timer.stop()

#stat nans
for c in df.columns:
    N_nans=num_nans(df,c)
    print("#Nans in {}={}M ({:2.1f}%)".format(c,N_nans/1e6,float(N_nans/N)*100))


#tract/patchs
print("#tracts={}".format(df.select("tract").distinct().count())) #check that all the tracts are present
df.groupBy(["tract","patch"]).count().groupBy("tract").count().\
withColumnRenamed("count","#patches").sort("tract").show() 

#density map
print("density map")
dens_map=projmap(df)
#plot
hp.gnomview(dens_map,rot=[55,-29.8],reso=reso,min=100,max=400,title=r"all $density/arcmin^2$")
plt.show()
#plt.savefig("newrun.png")

####
#qual
print("GALs good")
dfqual=df.filter((df.good==True) & (df.clean==True) &(df.extendedness>0.9)) 
Nqual=dfqual.count()
print("#good={}M {:2.1f}% raw".format(Nqual/1e6,Nqual/N*100))
for c in ['mag_i_cModel','blendedness']:
    N_nans=num_nans(dfqual,c)
    print("#Nans in {} ={}M ({:2.1f}%)".format(c,N_nans/1e6,float(N_nans/N)*100))

#density map
print("density map")
dens_map=projmap(dfqual)
#plot
hp.gnomview(dens_map,rot=[55,-29.8],reso=reso,min=0,max=300,title=r"good: $density/arcmin^2$")
plt.show()

######
#with i band information
print("GALs with i band")
dfqual_i=dfqual.select("ra","dec","mag_i_cModel","ipix").na.drop()
Nqual_i=dfqual_i.count()
print("GALs with i band= {}M".format(Nqual_i/1e6))
#histo
df_histplot(dfqual_i,'mag_i_cModel',doStat=True)
plt.show()

dens_map=projmap(dfqual_i)

hp.gnomview(dens_map,rot=[55,-29.8],reso=reso,min=0,max=200,title=r"good +i: $density/arcmin^2$")
plt.show()

#######
#i<24
print("GALs i <24")
df24=dfqual_i.filter(dfqual_i['mag_i_cModel']<24)
print("#gals i<24 ={}M".format(df24.count()/1e6))

dens_map=projmap(df24)

dens_map[dens_map==0]=hp.UNSEEN
A=np.sum(dens_map!=hp.UNSEEN)*pixarea
Nexp=40*10**(-0.36)
print("exp number={}/sq-arcmin tot={}".format(Nexp,Nexp*A))

w=(dens_map!=hp.UNSEEN)
plt.hist(dens_map[w],range=(0,20))
plt.xlabel("density i<24")
plt.axvline(Nexp)
plt.show()

hp.gnomview(dens_map,rot=[55,-29.8],reso=reso,min=0,max=20,title=r"i<24 $density/arcmin^2$")
plt.show()
