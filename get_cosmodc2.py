#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import randn
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

import healpy as hp
import matplotlib.pyplot as plt

####mystuff
import os,sys
sys.path.insert(0,"..")

from Timer import Timer
from df_tools import *
from qa_tools import *


#main
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

#
timer=Timer()
#
timer.start()
ff=os.environ['COSMODC2']
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
print("#partitions={}".format(df_all.rdd.getNumPartitions()))
df_all.printSchema()

#FILTER
#df=df_all.filter(df_all.halo_id>0)



#SELECTION
#cols="ra,dec,redshift"
#bands=['u','g','r','i','z','y']
#bands=['i']
#for b in bands:
#    s=",mag_{0},Mag_true_{0}_lsst_z0".format(b)
#    cols+=s
#use these columns
#df=df.select(cols.split(','))

# renommage
#for b in bands:
#    df=df.withColumnRenamed("Mag_true_{}_lsst_z0".format(b),"M{}".format(b))
#colbands=['b','g','r','y','m','k']

#shear
df=df_all.select('ra','dec','redshift','shear_1','shear_2','convergence','magnification')
df=df.withColumn("shear_phase",0.5*F.atan2(df.shear_2,df.shear_1))
df=df.withColumn("shear_amp",F.hypot(df.shear_2,df.shear_1))
df=df.withColumn("shear_amp",F.hypot(df.shear_2,df.shear_1))

df=df.withColumn("theta",F.radians(F.lit(90)-F.col("dec")))
df=df.withColumn("phi",F.radians(F.col("ra")))
df=df.withColumn("x",F.sin(df["theta"])*F.cos(df["phi"])).withColumn("y",F.sin(df["theta"])*F.sin(df["phi"])).withColumn("z",F.cos(df["theta"])).drop("theta","phi")
# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)

# nouvelles vars
#df=df.withColumn("g-r",df.mag_g-df.mag_r)
#df=df.withColumn("m-M",df.mag_r-df.Mr)
#df=df.withColumn("log10z",F.log10(df.redshift))
#df=df.withColumn("log10(Mstar)",F.log10(df.stellar_mass)).drop('stellar_mass')

print("After selection=")
df.printSchema()
print("#VARIABLES={} out of {} ({:3.1f}%)".format(len(df.columns),len(df_all.columns),float(len(df.columns))/len(df_all.columns)*100))

#re-filter
#df=df.sample(0.01)

#CACHE
#print("caching...")
#df=df.cache()

#gold_true=df.filter(df.mag_i<25.3).cache()
#print("size={} M".format(gold_true.count()/1e6))


timer.stop()

#cosmodc2
cen=[61.81579482165925,-35.20157446022967]

lon=cen[0]
lat=cen[1]
phi_c=deg2rad(lon)
theta_c=deg2rad(90-lat)
xc=sin(theta_c)*cos(phi_c)
yc=sin(theta_c)*sin(phi_c)
zc=cos(theta_c)


df=df.withColumn("r",F.hypot(df.x-xc,F.hypot(df.y-yc,df.z-zc))).drop("x","y","z")

#angular distance in degree
df=df.withColumn("angdist",F.degrees(2*F.asin(df.rr/2)))
