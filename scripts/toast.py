from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import StorageLevel

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


#DATA
#ff="/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_v4/dpdd_dc2_object_run1.2p.parquet"
#ff="/lsst/Run1.2/dpdd_dc2_object_run1.2p_v4.parquet"

ff=os.environ['RUN12P']
if 'MASTER' in os.environ.keys():
    ff=os.environ['MASTER']+'/'+ff

nside=2048
pixarea=hp.nside2pixarea(nside, degrees=True)*3600
reso= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
#we use pandas_udf because they are more efficient
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))



def tracts_outline(df):
    #list of tracts
    tracts=df.select("tract").distinct().toPandas()
    df_withpix=df.groupBy(["tract","ipix"]).count().cache()
    ipix=np.arange(hp.nside2npix(nside))
    pixborder=[]
    print("NSIDE={}".format(nside))
    for t in tracts['tract'].values :
        print("treating tract={}".format(t))
        #create a map just for this tract
        df_map=df_withpix.filter(df_withpix.tract==int(t))
        #create the healpix map
        tract_p=df_map.toPandas()
        #plt.hist(tract_p['count'].values)
        tract_map = np.full(hp.nside2npix(nside),hp.UNSEEN)
        tract_map[tract_p['ipix'].values]=1
        # for lit pixels compute the neighbours
        ipix1=tract_p['ipix'].values
        theta,phi=hp.pix2ang(nside,ipix1)
        neighbours=hp.get_all_neighbours(nside,theta,phi,0).transpose()
        # border if at least one neighbours is UNSEEN
        mask=[(hp.UNSEEN in tract_map[neighbours[pixel]]) for pixel in range(len(ipix1))]
        pixborder+=list(ipix1[mask])
    return pixborder


def get_borders_from_ipix(part):
    # Get the pixel ID from the iterator
    ipix = [*part]
    if len(ipix) == 0:
        # Empty partition
        yield []
    else:
        # Get the 8 neighbours of all unique pixels.
        theta, phi = hp.pix2ang(nside, ipix)
        neighbours = hp.get_all_neighbours(nside, theta, phi, 0).flatten()

        # Yield only pixels at the borders
        unseen = np.array([i for i in neighbours if i not in ipix])
        yield unseen


def tracts_outline2(df):
    #remove duplicate tract/ipix pairs
    df_pix=df.groupBy(["tract","ipix"]).count().drop('count')
    df_pix.show(5)
    #reapartition according to tract
    df_repart = df_pix.orderBy(df["tract"]).drop("tract").cache()
    df_repart.show(5)
    pix = df_repart.rdd.mapPartitions(get_borders_from_ipix).collect()
    return pix

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
    return skyMap

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



print("reading {}".format(ff))
df_all=spark.read.parquet(ff)


cols="tract,ra,dec,good,clean,extendedness"
for b in ['u','i']:
    s=",psFlux_flag_{0},psFlux_{0},psFluxErr_{0},mag_{0}_cModel,magerr_{0}_cModel,snr_{0}_cModel".format(b)
    cols+=s
print(cols)

#use these columns
df=df_all.select(cols.split(','))

#qual
df=df.filter((df.good==True) & (df.clean==True)&(df.extendedness==1)) 

#add a column of healpix indices
df=df.withColumn("ipix",Ang2Pix("ra","dec"))

#dont need these anymore
df=df.drop("good","clean","extendedness")

print("caching...")
print(df.columns)
df=df.cache()
#df=df.persist(StorageLevel.MEMORY_ONLY_SER)
print("N={}M".format(df.count()/1e6))

uddf=df.filter(df.ra.between(52.48,53.77)&df.dec.between(-28.66,-27.53))
print("sq uDDF N={}M".format(uddf.count()/1e6))

print(df.columns)

#cut stars: (df.psFluxErr_i<3))
