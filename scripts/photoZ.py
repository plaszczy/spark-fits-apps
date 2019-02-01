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


timer=Timer()
ddt=[]
    
ana="1: load(HDU)"
gal=spark.read.format("fits").option("hdu",1)\
  .load(ff)\
  .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))
    
gal.printSchema()
ddt.append(timer.step())
timer.print(ana)
#####
ana="2b: PZ full + show(5)"
trans=np.loadtxt('scripts/cum_inv.txt')

@pandas_udf('float', PandasUDFType.SCALAR)
def get_zrec(z,u):
        min=0.
        zmax=3.
        Nz=301
        step_z=(zmax-zmin)/Nz
        iz=np.array((z-zmin-step_z/2)/step_z,dtype='int')
        
        umin=0.
        umax=1.
        Nu=100
        step_u=(umax-umin)/Nu
        iu=np.array((u-umin-step_u/2)/step_u,dtype='int') 
    
        return pd.Series(trans[iz,iu])

#column of uniform randoms
gal=gal.withColumn("u",F.rand().astype('float'))
gal=gal.withColumn("zrec",get_zrec("z","u")).drop("u")
gal.show(5)
ddt.append(timer.step())
timer.print(ana)

####
ana="3: cache (count)"
gal=gal.cache()
print("N={}".format(gal.count()))
ddt.append(timer.step())
timer.print(ana)

#####
ana="4: statistics z"
gal.describe(['z','zrec']).show()
ddt.append(timer.step())
timer.print(ana)

ana="6: minmax z"
minmax=gal.select(F.min("z"),F.max("z")).first()
zmin=minmax[0]
zmax=minmax[1]
Nbins=100
dz=(zmax-zmin)/Nbins
ddt.append(timer.step())
timer.print(ana)

###############
ana="7: histo z"
#df on z 
#zbin=gal.select(gal.z,((gal['z']-zmin)/dz).astype('int').alias('bin'))
zbin=gal.select(gal.z,((gal['z']-zmin-dz/2)/dz).cast(IntegerType()).alias('bin'))
h=zbin.groupBy("bin").count().orderBy(F.asc("bin"))
p=h.select("bin",(zmin+dz/2+h['bin']*dz).alias('zbin'),"count").drop("bin").toPandas()
#p.to_csv("p.csv")

ddt.append(timer.step())
timer.print(ana)
    #
ana="histo z PZ"
import df_tools
p3=df_tools.hist_df(gal,"zrec",Nbins,bounds=minmax).toPandas()

#p3.to_csv("prec3.csv")
timer.print(ana)
