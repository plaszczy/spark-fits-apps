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

#cosmology LCDM-flat
c_speed=constants.c/1000.

H0=68.
Omega_M=0.306324

def integrand(z):
    return c_speed/H0/np.sqrt(Omega_M*(1+z)**3+(1-Omega_M))

def chi(z):
    return integrate.quad(integrand, 0.,z)[0]

chi_vec=np.vectorize(chi)

#main#########################################
parser = argparse.ArgumentParser(description='3D plot of galactic data')
    
parser.add_argument( "-v", help="increase verbosity",dest='verbose',action="store_true")
parser.add_argument( "-minmax", help="print minmax bounds then exits",dest='minmax',action="store_true")

parser.add_argument('-zname', dest='zname',help='Name of the redshift column',default="Z_COSMO")
parser.add_argument('-zmin', dest='zmin',type=float,help='Min redshift to cut',default=0.)
parser.add_argument('-zmax', dest='zmax',type=float,help='Max redshift to cut',default=0.1)

parser.add_argument('-raname', dest='raname',help='Name of the RA column',default="RA")
parser.add_argument('-ramin', dest='ramin',type=float,help='Min RA to cut',default=0.0)
parser.add_argument('-ramax', dest='ramax',type=float,help='Max RA to cut',default=360.)

parser.add_argument('-decname', dest='decname',help='Name of the DEC column',default="Dec")
parser.add_argument('-decmin', dest='decmin',type=float,help='Min DEC to cut',default=-90.)
parser.add_argument('-decmax', dest='decmax',type=float,help='Max DEC to cut',default=90.)

args = parser.parse_args(None)



ff=os.environ['FITSDIR']
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
gal=spark.read.format("fits").option("hdu",1)\
  .load(ff)\
  .select(F.col(args.raname).alias("RA"), F.col(args.decname).alias("Dec"), F.col(args.zname).alias("redshift"))
gal.printSchema()
timer.stop()


#filters
gal=gal.filter((gal.redshift>args.zmin)& (gal.redshift<args.zmax)) 

if args.ramin>0:
    gal=gal.filter(gal["RA"]>args.ramin) 
if args.ramax<360:
    gal=gal.filter(gal["RA"]<args.ramax) 

if args.decmin>-90:
    gal=gal.filter(gal["Dec"]>args.decmin) 
if args.decmax<90:
    gal=gal.filter(gal["Dec"]<args.decmax) 



#gal=gal.cache()
Ngal=gal.count()
print("Ndata={}M".format(Ngal/1e6))

#XYZ transform
#theta/phi is better than ra/dec
gal=gal.withColumn("theta",F.radians(90-gal['Dec'])).\
        withColumn("phi",F.radians(90-gal['RA']))

# distance is tricky
# LCDM planck (je crois)

#add r (linear intrep)
Nz=1000
zmax=3.
dz=zmax/(Nz-1)
ZZ=np.linspace(0,3,Nz)
CHI=chi_vec(ZZ)
#linear interp
@pandas_udf('float', PandasUDFType.SCALAR)
def dist_udf(z):
    i=np.array(z/dz,dtype='int')
    return pd.Series((CHI[i+1]-CHI[i])/dz*(z-ZZ[i])+CHI[i])

gal=gal.withColumn("r",dist_udf("redshift"))


# X,Y,Z
gal=gal.withColumn("X",gal.r*F.sin(gal.theta)*F.cos(gal.phi))\
  .withColumn("Y",gal.r*F.sin(gal.theta)*F.sin(gal.phi))\
  .withColumn("Z",gal.r*F.cos(gal.theta)).drop("theta").drop("phi")


if args.minmax:
    timer.start("minmax (put in cache)")
    gal=gal.cache()
    for col in gal.columns:
        m=minmax(gal,col)
        print("{:04.2f} < {} < {:04.2f}".format(m[0],col,m[1]))
    timer.stop()
    sys.exit


#DISPLAY
#protection
if Ngal>1e6:
    print("More than 1M points: are you sure to continue? (y/N))")
    answ='n'
    c=input()
    if not c=='y' :
        print("exiting")
        sys.exit()


# min/max redshift to color
#m=minmax(gal,"redshift")

#collect
timer.start("collecting data")
data=gal.select("X","Y","Z","redshift").collect()
timer.stop()

##3d plot
import inl
#inl.plot3D(data)
inl.plot3D_colored(data,width=1200,height=800)
