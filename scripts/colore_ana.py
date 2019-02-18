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

from time import time
class Timer:
    """
    a simple class for printing time (s) since last call
    """
    def __init__(self):
        self.t0=time()
        self.dt=0.

    def step(self):
        t1=time()
        self.dt=t1-self.t0
        self.t0=t1
        return self.dt

    def print(self,ana):
        print("-"*30)
        print(ana+"& {:2.1f} &".format(self.dt))
        print("-"*30)


def benchmark(ff):
    timer=Timer()
    ddt=[]
    
    ana="1: load(HDU)"

    gal=spark.read.format("fits").option("hdu",1)\
         .load(ff)\
         .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))

    
    #PARQUET="hdfs://134.158.75.222:8020/user/julien.peloton/LSST10Y_shuffled_uncomp"
    #gal=spark.read.parquet(PARQUET)\
    #  .select(F.col("RA"), F.col("DEC").alias("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))


    gal.printSchema()
    ddt.append(timer.step())
    timer.print(ana)
    #######
    ana="2: gauss PZ + show(5)"
    gal=gal.withColumn("zrec",(gal.z+0.03*(1+gal.z)*F.randn()).astype('float'))
    gal.show(5)
    ddt.append(timer.step())
    timer.print(ana)

    ####
    ana="3: cache (count)"
    gal=gal.cache()#.persist(StorageLevel.MEMORY_ONLY_SER)
    print("N={}".format(gal.count()))
    ddt.append(timer.step())
    timer.print(ana)

    #####
    ana="4: statistics z"
    gal.describe(['z']).show()
    ddt.append(timer.step())
    timer.print(ana)

    ana="5: statistics all"
    #get all statitics on z
    gal.describe().show()
    ddt.append(timer.step())
    timer.print(ana)

    ana="6: minmax"
    minmax=gal.select(F.min("z"),F.max("z")).first()
    zmin=minmax[0]
    zmax=minmax[1]
    Nbins=100
    dz=(zmax-zmin)/Nbins
    ddt.append(timer.step())
    timer.print(ana)

    ###############
    ana="7: histo df"
    #df on z 
    #zbin=gal.select(gal.z,((gal['z']-zmin)/dz).astype('int').alias('bin'))
    zbin=gal.select(gal.z,((gal['z']-zmin-dz/2)/dz).cast(IntegerType()).alias('bin'))
    h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
    p=h.select("bin",(zmin+dz/2+h['bin']*dz).alias('zbin'),"count").drop("bin").toPandas()
    #p.to_csv("p.csv")
    ddt.append(timer.step())
    timer.print(ana)
    #
    #ana="histo p3"
    #import df_tools
    #p3=df_tools.hist_df(gal,"zrec",Nbins,bounds=minmax).toPandas()
    #p3.to_csv("prec3.csv")
    #timer.print(ana)
    #p3.to_csv("prec3.csv")
    #ana="histo p5 (on the fly)"
    #p5=df_tools.hist_df(gal.withColumn("zrec2",gal.z+0.05*randn()*(1+gal.z)),"zrec2",Nbins,bounds=minmax).toPandas()
    #timer.print(ana)
    #p5.to_csv("prec5.csv")

    #ana="8a: histo (UDF)"
    #binNumber_udf=F.udf(lambda z: int((z-zmin)/dz))
    #p_udf=gal.select(gal.z,binNumber_udf(gal.z).alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
    #ddt.append(timer.step())
    #timer.print(ana)

    
    ana="8b: histo (pandas UDF)"
    @pandas_udf("float", PandasUDFType.SCALAR)
    def binFloat(z):
        return pd.Series((z-zmin)/dz)
    #dont know how to cast in pd so do it later
    p_udf=gal.select(gal.z,binFloat("z").astype('int').alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
    ddt.append(timer.step())
    timer.print(ana)


    #via rdd
    #ana="9: histo (rdd) reducebykey"
    #from operator import add
    #h=zbin.select("bin").rdd.map(lambda r:(r.bin,1)).reduceByKey(add).sortByKey().map(lambda x: (zmin+dz/2 +x[0]*dz,x[1]))
    #h=zbin.select("bin").rdd.map(lambda r:(r[0],1)).countByKey()
    #h.collect()
    #plt.plot(h.keys(),k,values())
    #ddt.append(timer.step())
    #timer.print(ana)

##    ana="10: RDD histogram"
##    #p_rdd=gal.select(gal.z).rdd.flatMap(list).histogram(Nbins)
##    p_rdd=gal.select(gal.z).rdd.map(lambda r: r.z).histogram(Nbins)
##    ddt.append(timer.step())
##    timer.print(ana)

   ## ana="11:tomographie"
##    shell=gal.filter(gal['zrec'].between(0.1,0.2))
##    nside=512
##    @pandas_udf('int', PandasUDFType.SCALAR)
##    def Ang2Pix(ra,dec):
##        return pd.Series(hp.ang2pix(nside,np.radians(90-dec),np.radians(ra)))
##    map=shell.select(Ang2Pix("RA","Dec").alias("ipix")).groupBy("ipix").count().toPandas()

    #back to python world
    #myMap = np.zeros(12 * nside**2)
    #myMap[map['ipix'].values]=map['count'].values

    #ddt.append(timer.step())
    #timer.print(ana)

    return ddt


###############


#main
import os
ff=os.environ['FITSDIR']

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

#single run
ddt=np.array(benchmark(ff))
with open("python_perf.txt", 'ab') as abc:
    np.savetxt(abc, ddt.reshape(1,ddt.shape[0]))
