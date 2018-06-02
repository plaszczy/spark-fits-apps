#input
#f="hdfs://134.158.75.222:8020//lsst/LSST10Y"
import os
f=os.environ.get("fitsdir","/home/plaszczy/fits/galbench_srcs_s1_0.fits")

#initialisations
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import StorageLevel


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

sqlContext = SQLContext.getOrCreate(sc)

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

from time import time
t0=time()
def time_me(s):
    global t0
    t1=time()
    print("-"*30)
    print(ana+"& {:2.1f} \\".format(t1-t0))
    print("-"*30)
    t0=t1

ana="load (HDU)+show"
from pyspark.sql import functions as F
gal=spark.read.format("com.sparkfits").option("hdu",1)\
     .load(f)\
     .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))\
     .persist(StorageLevel.MEMORY_ONLY_SER)
#     .cache()

gal.printSchema()
gal.columns
gal.dtypes
gal.show(5)

time_me(ana)

ana="count"
print("N={}".format(gal.count()))
time_me(ana)

ana="stat z"
gal.describe(['z']).show()
time_me(ana)


ana="stats all"
#get all statitics on z
gal.describe().show()
time_me(ana)

ana="minmax"
minmax=gal.select(F.min("z"),F.max("z")).first()
zmin=minmax[0]
zmax=minmax[1]
#
Nbins=100
dz=(zmax-zmin)/Nbins
time_me(ana)

ana="dN/Dz histogram"
#df
#zbin=gal.select(gal.z,((gal['z']-zmin)/dz).astype('int').alias('bin')).cache()
from pyspark.sql.types import IntegerType
zbin=gal.select(gal.z,((gal['z']-zmin)/dz).cast(IntegerType()).alias('bin')).cache()
#via udf
#binNumber=F.udf(lambda z: int((z-zmin)/dz))
#zbin=gal.select(gal.z,binNumber(gal.z).alias('bin')).cache()
#via rdd
#zbin=gal.select("z").rdd.map(lambda z: (z[0],int((z[0]-zmin)/dz))).cache()

h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
histo=h.toPandas()
import matplotlib.pyplot as plt
import numpy as np
#plt.plot(x,y)

#via rdd
#from operator import add
#h=zbin.select("bin").rdd.map(lambda r:(r[0],1)).reduceByKey(add)
#h=zbin.select("bin").rdd.map(lambda r:(r[0],1)).countByKey()
#plt.plot(h.keys(),k,values())
time_me(ana)

ana="add gausian smearing"
from pyspark.sql.functions import randn
gal=gal.withColumn("zrec",gal.z+0.03*(1+gal.z)*randn()).cache()
gal.show()
minmax=gal.select(F.min("zrec"),F.max("zrec")).first()
zmin=minmax[0]
zmax=minmax[1]
time_me(ana)

ana="histo2"
from hist_spark import hist_spark
hrec=hist_spark(gal,"zrec",Nbins)
time_me(ana)

ana="tomographie"
shell=gal.filter(gal['z'].between(0.1,0.2))
time_me(ana)
