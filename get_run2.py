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

doCounts=False

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
ff=os.environ['RUN22']
print("input={}".format(ff))
df_all=spark.read.parquet(ff)
print("#partitions={}".format(df_all.rdd.getNumPartitions()))
#df_all.printSchema()

if doCounts:
    print("|| all || {:.1f} ||".format(df_all.count()/1e6))

#FILTER good
df=df_all.filter((df_all.good==1)&(df_all.clean==1))

#string match
#[s for s in df_all.columns if "PSF" in s or "psf" in s]



#COLUMNS
cols="ra,dec,extendedness,blendedness"
for b in ['i']:
#for b in ['u','g','r','i','z','y']:
    s=",mag_{0},mag_{0}_cModel,magerr_{0}_cModel,snr_{0}_cModel,psf_fwhm_{0}".format(b)
    cols+=s

#use these columns
df=df.select(cols.split(','))


# ADD HEALPIXELS
print('add healpixels')
df=add_healpixels(df)
print("nside={}".format(nside))

if doCounts:
    print("|| good || {:.1f} ||".format(df.count()/1e6))


#print("After selection=")
#df.printSchema()
#print("#VARIABLES={} out of {} ({:3.1f}%)".format(len(df.columns),len(df_all.columns),float(len(df.columns))/len(df_all.columns)*100))

#re-filter
#df=df.sample(0.01)

gal=df.filter(df.extendedness==1).drop("extendedness")


print("gal=",gal.columns)

if doCounts:
    print("|| gal (ext=1) || {:.1f} ||".format(gal.count()/1e6))


#CACHE
print("caching gal...")
gal=gal.cache()

#print("tot size={} M".format(gal.count()/1e6))
#print("i size={} M, i<24={}".format(i.count()/1e6,i24.count()/1e6))


#then  cut on mag_cModel

#gold
#gold=gal.select("ipix","ra","dec","mag_i","psFlux_i","psFluxErr_i","psf_fwhm_i").na.drop().filter("mag_i<25.3")
#if doCounts:
#    print("|| mag_i<25.3 || {:.1f} ||".format(gold.count()/1e6))

gold=gal.select("ipix","ra","dec","mag_i_cModel","magerr_i_cModel","psf_fwhm_i","snr_i_cModel","blendedness").filter("mag_i_cModel<24").na.drop()
#gold=gold.cache()

if doCounts:
    print("|| mag_i_cModel<24 || {:.1f} ||".format(gold.count()/1e6))

gold5=gold.filter(gold.snr_i_cModel>5)
gold10=gold5.filter(gold5.snr_i_cModel>10)

if doCounts:
    print("|| SNR_i>5 || {:.1f} ||".format(gold5.count()/1e6))
    print("|| SNR_i>10 || {:.1f} ||".format(gold10.count()/1e6))


goldust=gold10.filter(gold10.blendedness<10**(-0.375))
if doCounts:
    print("|| unblended || {:.1f} ||".format(goldust.count()/1e6))

iqual=gold5.filter((gold5.mag_i_cModel<24) & (gold5.blendedness<10**(-0.375))).drop("blendedness")
print("iqual=",iqual.columns)

#if  not doCounts:
#    print("#gals(i<24)={}M".format(iqual.cache().count()/1e6))


timer.stop()

#centre
cen=[61.89355123721637,-36.006714393702175]


# from qa_tools import *
#sky=densitymap(iqual,rot=cen,xsize=350)

#from healpy import *
#from matplotlib import pyplot as plt
#plt.figure()
#plt.hist(sky[sky != UNSEEN],bins=100)

#stars
#sstars=df.filter(df.extendedness==0).select("ipix","ra","dec","mag_i").na.drop()
#sstars=sstars.filter(~sstars['mag_i'].contains(np.inf))
#sstars=sstars.filter(sstars['mag_i']<24).drop("mag_i")
#print("#stars={}M".format(sstars.cache().count()/1e6))

#concat both
#galstars=iqual.select("ipix","ra","dec").union(sstars)
#print("#galstars={}M".format(galstars.cache().count()/1e6))
