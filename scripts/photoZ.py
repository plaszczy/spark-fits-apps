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
        print(ana+": {:2.1f}s".format(self.dt))
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

##### gauss
gal=gal.withColumn("zrec_g",(gal.z+0.03*(1+gal.z)*F.randn()).astype('float'))

####full PZ
ana="2b: PZ full + show(5)"

# read the inverse-cumulative file 
cuminv=np.loadtxt('scripts/cuminv_gauss.txt')
#cuminv=np.loadtxt('scripts/cuminv_gauss.txt')
#cuminv=np.loadtxt('scripts/cuminv_bdt.txt')
# we know the binnings that were used
dz=0.01
du=1/1000.

#find indices and return the table values
@pandas_udf('float', PandasUDFType.SCALAR)
def z_PZ(zr,u):
    iz=np.array(zr/dz,dtype='int')
    iu=np.array(u/du,dtype='int') 
    return pd.Series(cuminv[iz,iu])


#add column of uniform random numbers
gal=gal.withColumn("u",F.rand())

#transform with the inverse-cumulative table
gal=gal.withColumn("zrec",z_PZ("z","u")+dz/2)\
       .drop("u")  #do not need u anymore

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
gal.describe(['z','zrec_g','zrec']).show()
ddt.append(timer.step())
timer.print(ana)

###############
ana="7: histo z"
zmin=0.
zmax=2.4
Nbins=100

from df_tools import df_hist
h,dz,b=df_hist(gal,'z',Nbins,bounds=(zmin,zmax))
p=h.toPandas()
#p.to_csv("p.csv")

ddt.append(timer.step())
timer.print(ana)

ana='histo z_g'
h,dz,b=df_hist(gal,'zrec_g',Nbins,bounds=(zmin,zmax))
p3=h.toPandas()
ddt.append(timer.step())
timer.print(ana)

ana='histo z_PZ'
h,dz,b=df_hist(gal,'zrec',Nbins,bounds=(zmin,zmax))
p3_PZ=h.toPandas()
ddt.append(timer.step())
timer.print(ana)
