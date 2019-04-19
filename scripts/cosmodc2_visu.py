#initialisations

#spark
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType

#standrd python
import os,sys
import numpy as np
import pandas as pd
import healpy as hp

import argparse
from scipy import constants,integrate

#mystuff
sys.path.insert(0,os.path.join(os.getcwd(),".."))
from Timer import *
from df_tools import *

#main#########################################
parser = argparse.ArgumentParser(description='3D plot of galactic data')
    
parser.add_argument( "-v", help="increase verbosity",dest='verbose',action="store_true")
parser.add_argument( "-minmax", help="print minmax bounds then exits",dest='minmax',action="store_true")

parser.add_argument('-zname', dest='zname',help='Name of the redshift column',default="Z_COSMO")
parser.add_argument('-zmin', dest='zmin',type=float,help='Min redshift to cut',default=0.)
parser.add_argument('-zmax', dest='zmax',type=float,help='Max redshift to cut',default=3)

parser.add_argument('-raname', dest='raname',help='Name of the RA column',default="RA")
parser.add_argument('-ramin', dest='ramin',type=float,help='Min RA to cut',default=0.0)
parser.add_argument('-ramax', dest='ramax',type=float,help='Max RA to cut',default=360.)

parser.add_argument('-decname', dest='decname',help='Name of the DEC column',default="Dec")
parser.add_argument('-decmin', dest='decmin',type=float,help='Min DEC to cut',default=-90.)
parser.add_argument('-decmax', dest='decmax',type=float,help='Max DEC to cut',default=90.)

args = parser.parse_args(None)



ff=os.environ['COSMODC2']
print("Working on: "+ff)

#init spark
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)


timer=Timer()
timer.start("loading")   
df=spark.read.parquet(ff)
timer.stop()


#filters
df=df.filter("halo_id>0")

df=df.filter((df.redshift>args.zmin)& (df.redshift<args.zmax)) 
if args.ramin>0:
    df=df.filter(df["ra"]>args.ramin) 
if args.ramax<360:
    df=df.filter(df["ra"]<args.ramax) 

if args.decmin>-90:
    df=df.filter(df["dec"]>args.decmin) 
if args.decmax<90:
    df=df.filter(df["dec"]<args.decmax) 


#add abs flux
#df=df.withColumn("AbsFlux",F.pow(F.lit(10),-0.4*df.Mag_true_g_lsst_z0))

#large halos selection

timer.start("add halo occupancy")   
df_halo=df.groupBy("halo_id").count().cache()
df=df.join(df_halo,"halo_id").withColumnRenamed("count","halo_members")
timer.stop()

df=df.filter(df.halo_members>50)

df=df.cache()
Ndf=df.count()
print("Ndata={}M".format(Ndf/1e6))

if args.minmax:
    timer.start("minmax (put in cache)")
    df=df.cache()
    for col in df.columns:
        m=minmax(df,col)
        print("{:04.2f} < {} < {:04.2f}".format(m[0],col,m[1]))
    timer.stop()
    sys.exit




#DISPLAY
#protection
if Ndf>1e6:
    print("More than 1M points: are you sure to continue? (y/N))")
    answ='n'
    c=input()
    if not c=='y' :
        print("exiting")
        sys.exit()


#collect
timer.start("collecting data")
data=df.select("position_x","position_y","position_z","Mag_true_g_lsst_z0","size_true").collect()
timer.stop()



##3d plot
import inl
inl.plot3D(data,client=True)
#inl.plot4D(data,col_index=3,col_minmax=(-22.,-14.),client=True)
