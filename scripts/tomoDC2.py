#initialisations
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
import healpy as hp

import sys,os
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from df_tools import *

###############

#inits
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#ang2pix
nside=4096
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
        return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

#data source
#ff=os.path.join(os.environ['COSMODC2'],"xyz_v1.1.4.parquet")
ff=os.environ['COSMODC2']
print("input={}".format(ff))
gal=spark.read.parquet(ff)
gal.printSchema()

#columns
gal=gal.select("ra","dec","redshift")

#### no need for quality cuts this is thruth

#cache
print("N={}".format(gal.cache().count()))
####

import numpy as np

zshell=np.linspace(0,3,16)


#writemap
write=False
for i in range(len(zshell)-1):
    z1=zshell[i]
    z2=zshell[i+1]
    shell=gal.filter(gal['redshift'].between(z1,z2))
    print("shell=[{:.2f},{:.2f}] N={}M".format(z1,z2,shell.count()/1e6))
    #add col of healpixels
    shell=shell.withColumn("ipix",Ang2Pix("ra","dec"))
    df_map=shell.groupBy("ipix").count()
    #histo counts
    df_map.describe(['count']).show()
    #df_histplot(df_map,"count")   
    #back to python world
    p_map=df_map.toPandas()
    Nbar=np.mean(p_map['count'])
    myMap=np.full(hp.nside2npix(nside),hp.UNSEEN)
    myMap[p_map['ipix'].values]=p_map['count'].values/Nbar-1
    hp.gnomview(myMap,rot=[65.36,-39.55],reso=hp.nside2resol(nside,arcmin=True),cmap='jet',title=r"z$\in$[{:.1f},{:.1f}]".format(z1,z2),min=-0.4,max=0.4,xsize=500)
    if write:
        p_map.to_hdf("map_{}.hdf5".format(i),"w")
        #hp.write_map("map{:02d}.fits".format(i), myMap)
