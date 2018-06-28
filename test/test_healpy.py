from pyspark.sql.functions import udf,pandas_udf, PandasUDFType

import os
f="file://"+os.path.join(os.getcwd(),"tests/test_file.fits")
print(f)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

gal=spark.read.format("com.sparkfits").option("hdu",1).load(f).cache()

N=gal.count()

import healpy as hp
import numpy as np
import pandas as pd

#RDD
#nside=512
#rdd=gal.rdd.map(lambda r: hp.ang2pix(512,np.radians(90-r['Dec']),np.radians(r['RA'])))
#hitcount = rdd.map(lambda x: (x, 1))
#myPartialMap = hitcount.countByKey()
#myMap = np.zeros(12 * nside**2)
#myMap[list(myPartialMap.keys())] = list(myPartialMap.values())
#print("RDD OK")

#UDF
test_0=udf(lambda ra,dec: (90-dec)-(ra))
gal.withColumn("0",test_0(gal['RA'],gal['Dec'])).show()

print("TEST0 OK")

@udf('float')
def plus_one(v):
      return v + 1

#numpy
test_np=udf(lambda ra,dec: np.radians(90-dec)-np.radians(ra))
#gal.withColumn("np",test_np(gal['RA'],gal['Dec'])).show()
gal.select(plus_one("RA")).show()

@pandas_udf('float', PandasUDFType.SCALAR)
def toRad(v):
    return pd.Series(np.radians(v))


@pandas_udf('int', PandasUDFType.SCALAR)
def Ang2Pix(ra,dec):
    return pd.Series(hp.ang2pix(512,np.radians(90-dec),np.radians(ra)))

