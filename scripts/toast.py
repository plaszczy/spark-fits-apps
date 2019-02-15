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


nside=2048
pixarea=hp.nside2pixarea(nside, degrees=True)*3600
reso= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))


def projmap(df,col,minmax=None):
    df_map=df.select(col,"ipix").na.drop().groupBy("ipix").avg(col)
    #statistics per pixel
    var=df_map.columns[-1]
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
    hp.gnomview(skyMap,rot=[55,-29.8],reso=reso,min=minmax[0],max=minmax[1],title=var)
    plt.show()

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
    hp.gnomview(skyMap,rot=[55,-29.8],reso=reso,min=minmax[0],max=minmax[1],title=r"$density/arcmin^2$")
    plt.show()




# Initialise our Spark session
spark = SparkSession.builder.getOrCreate()
print("spark session started")


# input
ff="/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_v4/dpdd_object_run1.2p.parquet"
print("reading {}".format(ff))
df_all=spark.read.parquet(ff)


cols="ra,dec,good,clean,extendedness"
for b in ['u','g','r','i','z','y']:
    s=",psFlux_{0},mag_{0},mag_{0}_cModel".format(b)
    cols+=s
print(cols)

#use these columns
df=df_all.select(cols.split(','))

#qual
df=df.filter((df.good==True) & (df.clean==True) & (df.extendedness>0.9)) 

#add a column of healpix indices
df=df.withColumn("ipix",Ang2Pix("ra","dec"))

#dont need these anymore
df=df.drop("ra","dec","good","clean","extendedness")

print("caching...")
print(df.columns)
df=df.cache()
print("N={}M".format(df.count()/1e6))
