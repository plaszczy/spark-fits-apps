
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from pyspark.sql import SQLContext,Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from time import time


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

sqlContext = SQLContext.getOrCreate(sc)

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)


#read fits files
gal=spark.read.format("com.sparkfits").option("hdu",1)\
     .load("/home/plaszczy/fits/galbench_srcs_s1_0.fits")\
     .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))\
     .cache()
#    .persist(StorageLevel.MEMORY_ONLY_SER)

gal.printSchema()
gal.show(10)

#get all statitics on z
stat=gal.describe(['z'])

gal.summary().show()
#get some base statitics on z
gal.select(F.mean(gal.z),F.min(gal.z),F.max(gal.z)).show()

#gal.cor

#histograms
#win = Window.partitionBy('z')
#gal.select(F.count('z').over(win).alias('histogram'))

#get minmax
minmax=gal.select(F.min("z"),F.max("z")).first()
zmin=minmax[0]
zmax=minmax[1]
#
minmax=gal.select("z").summary("min", "max").collect()
zmin=float(minmax[0].z)
zmax=float(minmax[1].z)

#
Nbins=100
dz=(zmax-zmin)/Nbins

# add bin column
#df
gal.select(gal.z,((gal['z']-zmin)/dz).astype('int').alias('bin')).show()
zbin=gal.select(gal.z,((gal['z']-zmin)/dz).cast(IntegerType()).alias('bin'))
#udf
binNumber=F.udf(lambda z: int((z-zmin)/dz))
gal.select(gal.z,binNumber(gal.z).alias('bin'))

#rdd
gal.select("z").rdd.map(lambda z: (z[0],int((z[0]-zmin)/dz))).take(10)

#count
h=zbin.groupBy("bin").count().orderBy(F.asc("bin")).collect()

#rdd



#add gaussian smearing
from pyspark.sql.functions import rand, randn

#tomographie
shell=gal.filter(gal['z'].between(0.1,0.2))


#histo scala cf spark-fits-app (scala et python)
# val df_indexed = jc.df_index
#                       .map(x => (jc.grid.index(dec2theta(x.dec), ra2phi(x.ra)), x.z, 1))
#val result = df_indexed.filter(x => x._2 >= start && x._2 < stop) // filter in redshift space
#.groupBy("_1").agg(sum($"_3")) // group by pixel index and make an histogram
#.count()

#come back to numpy world
