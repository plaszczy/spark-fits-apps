import os
f="tests/test_file.fits"

#initialisations
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import StorageLevel


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext


from pyspark.sql import functions as F
gal=spark.read.format("com.sparkfits").option("hdu",1).load(f).cache()

import healpy as hp
import numpy as np


#RDD
nside=512
rdd=gal.rdd.map(lambda r: hp.ang2pix(512,np.radians(90-r['Dec']),np.radians(r['RA'])))
hitcount = rdd.map(lambda x: (x, 1))
myPartialMap = hitcount.countByKey()
myMap = np.zeros(12 * nside**2)
myMap[list(myPartialMap.keys())] = list(myPartialMap.values())

print("RDD OK")

#UDF
test_0=F.udf(lambda ra,dec: (90-dec)-(ra))
gal.withColumn("0",test_0(gal['RA'],gal['Dec'])).show()

print("TEST0 OK")


#numpy
test_np=F.udf(lambda ra,dec: np.radians(90-dec)-np.radians(ra))
gal.withColumn("np",test_np(gal['RA'],gal['Dec'])).show()

print("NUMPY OK")

