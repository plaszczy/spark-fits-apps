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
ff=os.environ['RUN2']
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
print("#partitions={}".format(df_all.rdd.getNumPartitions()))
df_all.printSchema()


#FILTER good
df=df_all.filter((df_all.good==1)&(df_all.clean==1))

#string match
#[s for s in df_all.columns if "PSF" in s or "psf" in s]



#COLUMNS
cols="ra,dec,extendedness,blendedness,IxxPSF_i,IxyPSF_i,psf_fwhm_i,IyyPSF_i"
for b in ['i']:
#for b in ['u','g','r','i','z','y']:
    s=",psFlux_flag_{0},psFlux_{0},psFluxErr_{0},mag_{0},mag_{0}_cModel,magerr_{0}_cModel,snr_{0}_cModel".format(b)
    cols+=s
print(cols)

#use these columns
df=df.select(cols.split(','))

colbands=['b','g','r','y','m','k']


# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)


print("After selection=")
df.printSchema()
print("#VARIABLES={} out of {} ({:3.1f}%)".format(len(df.columns),len(df_all.columns),float(len(df.columns))/len(df_all.columns)*100))

#re-filter
#df=df.sample(0.01)

gal=df.filter(df.extendedness==1).drop("extendedness")


#i<24
#band="i"
#cols="ipix,blendedness,psFlux_{0},psFluxErr_{0},mag_{0}_cModel,magerr_{0}_cModel,snr_{0}_cModel".format(band)
#i=gal.filter(df["psFlux_flag_{}".format(band)]==False).select(cols.split(",")).na.drop()
#iqual=i.filter((i['blendedness']<10**(-0.375)) & (i['snr_i_cModel']>5))
#i24=iqual.filter(iqual.mag_i_cModel<24)


#gold
gold=gal.select("ra","dec","mag_i","psFlux_i","psFluxErr_i",'psf_fwhm_i','IxxPSF_i','IxyPSF_i','IyyPSF_i').na.drop().filter("mag_i<25.3")
gold_cModel=gal.select("ra","dec","mag_i_cModel","magerr_i_cModel","psFlux_i","psFluxErr_i",'psf_fwhm_i','IxxPSF_i','IxyPSF_i','IyyPSF_i').na.drop().filter("mag_i_cModel<25.3")


#CACHE
print("caching...")
df=df.cache()

print("tot size={} M, gals={}".format(df.count()/1e6,gal.count()/1e6))
#print("i size={} M, i<24={}".format(i.count()/1e6,i24.count()/1e6))


timer.stop()

#centre
rot=[61.89355123721637,-36.006714393702175]
