#initialisations
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
import healpy as hp
from matplotlib import pyplot as plt

import sys,os
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import *

###############

#inits
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#ang2pix
nside=512


@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
        return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

def readdata():
        #data source
        ff=os.environ['SKYSIM']
        print("input={}".format(ff))
        gal=spark.read.parquet(ff)
        gal.printSchema()
        #columns
        gal=gal.select("ra","dec","redshift").withColumn("ipix",Ang2Pix("ra","dec"))
        print("N={}".format(gal.cache().count()))
        return gal

#zup=np.linspace(0,3,31)
#zup=np.linspace(0,3,16)
def tomo(gal,zup,write=False):
        z=zup[::-1]
        i=0
        for (z2,z1) in zip(z[:-1],np.roll(z,-1)):
                shell=gal.filter(gal['redshift'].between(z1,z2))
                print("shell=[{:.2f},{:.2f}] N={}M".format(z1,z2,shell.count()/1e6))
                df_map=shell.groupBy("ipix").count()
                #histo counts
                #df_map.describe(['count']).show()
                #df_histplot(df_map,"count")   
                #back to python world
                p_map=df_map.toPandas()
                Nbar=np.mean(p_map['count'])
                myMap=np.full(hp.nside2npix(nside),hp.UNSEEN)
                myMap[p_map['ipix'].values]=p_map['count'].values/Nbar-1
                #hp.orthview(myMap,half_sky=True,rot=cen,cmap='jet',title=r"z$\in$[{:.1f},{:.1f}]".format(z1,z2),min=-0.4,max=0.4)
                if write:
                        #p_map.to_hdf("map_{}.hdf5".format(i),"w")
                        hp.write_map("map{:02d}.fits".format(i), myMap)
                i=i+1
#zup=np.linspace(0,3,16)
def displaymaps(zup,cen=[45.,-44.],cmap='inferno'):
        z=zup[::-1]
        i=0
        for (z2,z1) in zip(z[:-1],np.roll(z,-1)):
                myMap=hp.read_map("map{:02d}.fits".format(i))
                fig=plt.figure(figsize=(10,10))
                hp.orthview(myMap,fig=1,half_sky=True,cbar=False,rot=cen,cmap=cmap,title=r"{:.1f}<z<{:.1f}".format(z1,z2),min=-0.4,max=1,margins=(0,0,0,0))
                plt.savefig("map{:02d}.png".format(i))
                plt.close(1)
                i=i+1
