from pyspark.sql import SparkSession

from pyspark.sql.functions import pandas_udf, PandasUDFType
import numpy as np
import pandas as pd
import healpy as hp
from matplotlib import pyplot as plt

#nside=131072
nside=1024
nest=False

pixarea=hp.nside2pixarea(nside, degrees=True)*3600
reso= hp.nside2resol(nside,arcmin=True)
#create the ang2pix user-defined-function. 
@pandas_udf('long', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra),nest=nest))
##################

# req: ipix,tract
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

def projmap_mean(df,col,minmax=None,dohist=True,**kwargs ):
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

    if dohist:
        plt.hist(map_p[var].values,bins=80,range=minmax)
        plt.xlabel(var)

    hp.gnomview(skyMap,reso=reso,min=minmax[0],max=minmax[1],nest=nest,title=var,**kwargs )
    plt.show()
    return skyMap

def projmap_max(df,col,minmax=None,dohist=True,**kwargs ):
    df_map=df.select(col,"ipix").na.drop().groupBy("ipix").max(col)
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

    if dohist:
        plt.hist(map_p[var].values,bins=80,range=minmax)
        plt.xlabel(var)

    hp.gnomview(skyMap,nest=nest,reso=reso,min=minmax[0],max=minmax[1],title=var,**kwargs )
    plt.show()
    return skyMap

#from spark v2.4.0 only
@pandas_udf('float', PandasUDFType.GROUPED_AGG)  # doctest: +SKIP
def median_udf(v):
    return np.median(v)


def projmap_median_udf(df,col,minmax=None,**kwargs ):
    df_map=df.select(col,"ipix").na.drop().groupBy("ipix").agg(median_udf(col))
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
    hp.gnomview(skyMap,nest=nest,reso=reso,min=minmax[0],max=minmax[1],title=var,**kwargs )
    plt.show()
    return skyMap


def projmap_median(df,col,minmax=None,**kwargs ):
    rdd=df.select("ipix",col).na.drop().rdd.map(lambda r: (r[0],r[1]))
    map_rdd=rdd.groupByKey().mapValues(tuple).mapValues(np.median)
    #collect renvoie un eliste de tuples
    #back to pandas
    map_p=map_rdd.map(lambda r: (r[0],float(r[1]))).toDF().toPandas()
    val=np.array(map_p._2.values)
    mu=val.mean()
    sig=val.std()
    if minmax==None:
        minmax=(np.max([0,mu-2*sig]),mu+2*sig)

    print("N={} \nmean={} \nsigma={}\nmin={}\nmax={}".format(val.size,mu,sig,min(val),max(val)))
    skyMap= np.full(hp.nside2npix(nside),hp.UNSEEN)
    skyMap[map_p._1.values]=val
    hp.gnomview(skyMap,nest=nest,reso=reso,min=minmax[0],max=minmax[1],title="median("+col+")",**kwargs)
    plt.show()
    return skyMap



def countsmap(df,minmax=None,**kwargs):
    assert "ipix" in df.columns
    df_map=df.select("ipix").groupBy("ipix").count()

    #back to python world
    map_p=df_map.toPandas()
    A=map_p.index.size*pixarea/3600
    print("map area={} deg2".format(A))
    #statistics per pixel
    var='count'
    s=df_map.describe([var])
    s.show()
    r=s.select(var).take(3)
    N=int(r[0][0])
    mu=float(r[1][0])
    sig=float(r[2][0])

    #now data is reduced create the healpy map
    skyMap= np.full(hp.nside2npix(nside),hp.UNSEEN)
    skyMap[map_p['ipix'].values]=map_p[var].values
    
    if minmax==None:
        minmax=(np.max([0,mu-2*sig]),mu+2*sig)
    hp.gnomview(skyMap,nest=nest,reso=reso,min=minmax[0],max=minmax[1],title="density/pixel (nside={})".format(nside),**kwargs)
    plt.show()
    return skyMap


def densitymap(df,minmax=None,**kwargs):
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

    #now data is reduced create the healpy map
    skyMap= np.full(hp.nside2npix(nside),hp.UNSEEN)
    skyMap[map_p['ipix'].values]=map_p[var].values
    
    if minmax==None:
        minmax=(np.max([0,mu-2*sig]),mu+2*sig)
    hp.gnomview(skyMap,nest=nest,reso=reso,min=minmax[0],max=minmax[1],title=r"$density/arcmin^2$",**kwargs)
    plt.show()
    return skyMap

def add_healpixels(df,ra="ra",dec="dec"):
    assert ra in df.columns
    assert dec in df.columns
    print("NSIDE={}, pixel area={} arcmin^2, resolution={} arcmin".format(nside,pixarea,reso))
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.broadcast(nside) 
    df=df.withColumn("ipix",Ang2Pix("ra","dec"))
    return df
