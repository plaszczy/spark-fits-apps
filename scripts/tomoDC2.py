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

###############

#inits
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

#ang2pix
nside=2048
@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
        return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))

#data source
import os
ff=os.path.join(os.environ['COSMODC2'],"xyz_v1.1.4.parquet")
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
zshell=np.linspace(0,3,10)[::-1]

#writemap
write=False
for i in range(len(zshell)-1):
#for i in [0,]:
    z2=zshell[i]
    z1=zshell[i+1]
    shell=gal.filter(gal['redshift'].between(z1,z2))
    print("shell=[{:.2f},{:.2f}] N={}M".format(z1,z2,shell.count()/1e6))
    #add col of healpixels
    shell=shell.withColumn("ipix",Ang2Pix("ra","dec"))
    df_map=shell.groupBy("ipix").count()
    #histo counts
    df_map.describe(['count']).show()
    #back to python world
    p_map=df_map.toPandas()
    myMap=np.full(hp.nside2npix(nside),hp.UNSEEN)
    myMap[p_map['ipix'].values]=p_map['count'].values
    hp.gnomview(myMap,rot=[65.36,-39.55],reso=hp.nside2resol(nside,arcmin=True),xsize=500,cmap='jet',title=r"z$\in$[{:.2f},{:.2f}]".format(z1,z2))
    if write:
        hp.write_map("map{:02d}.fits".format(i), myMap)
