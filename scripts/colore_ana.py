#input
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
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()

    def print(self,ana):
        t1=time()
        print("-"*30)
        print(ana+"& {:2.1f} &".format(t1-self.t0))
        print("-"*30)
        self.t0=t1
        ana="load (HDU)+show"


timer=Timer()

#######
ana="load(HDU)"
from pyspark.sql import functions as F
gal=spark.read.format("com.sparkfits").option("hdu",1)\
     .load(f)\
     .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))
#     .cache()
#     .persist(StorageLevel.MEMORY_ONLY_SER)

gal.printSchema()
timer.print(ana)
#######
ana="PZ + show(5)"
from pyspark.sql.functions import randn
gal=gal.withColumn("zrec",(gal.z+0.03*(1+gal.z)*randn()).astype('float'))
gal.show(5)
timer.print(ana)

####
ana="cache (count)"
print("N={}".format(gal.cache().count()))
timer.print(ana)

#####
ana="statistics z"
gal.describe(['z','zrec']).show()
timer.print(ana)

ana="statistics all"
#get all statitics on z
gal.describe().show()
timer.print(ana)

ana="minmax"
minmax=gal.select(F.min("z"),F.max("z")).first()
zmin=minmax[0]
zmax=minmax[1]
Nbins=100
dz=(zmax-zmin)/Nbins
timer.print(ana)


###############
ana="histo (df)"
#df
#zbin=gal.select(gal.z,((gal['z']-zmin)/dz).astype('int').alias('bin'))
from pyspark.sql.types import IntegerType
zbin=gal.select(gal.z,((gal['z']-zmin)/dz).cast(IntegerType()).alias('bin'))
h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
hh=h.select("bin",(zmin+dz/2+h['bin']*dz).alias('zbin'),"count").drop("bin")
hh.toPandas().to_csv("p.csv")


#import matplotlib.pyplot as plt
#plt.plot(zmin+dz/2+p['bin']*dz,p['count'])
#plt.bar(zmin+dz/2+p['bin']*dz,p['count'],width=dz)
#plt.show()

timer.print(ana)

#
ana="histo rec"

from hist_df import hist_df
hrec=hist_df(gal,"zrec",Nbins,zmin=zmin,zmax=zmax)
timer.print(ana)
hrec.toPandas().to_csv("prec.csv")

import sys
sys.exit()

ana="histo (UDF)"
binNumber=F.udf(lambda z: int((z-zmin)/dz))
p_udf=gal.select(gal.z,binNumber(gal.z).alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
timer.print(ana)

ana="histo (rdd)"
#via rdd
#from operator import add
#h=zbin.select("bin").rdd.map(lambda r:(r[0],1)).reduceByKey(add)
#h=zbin.select("bin").rdd.map(lambda r:(r[0],1)).countByKey()
#plt.plot(h.keys(),k,values())

p_rdd=gal.select(gal.z).rdd.flatMap(list).histogram(Nbins)
timer.print(ana)

###############
ana="tomographie"
shell=gal.filter(gal['z'].between(0.1,0.2))
timer.print(ana)
