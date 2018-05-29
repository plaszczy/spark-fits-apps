
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from pyspark.sql import functions as F

from time import time


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)


#read fits files
gal=spark.read.format("com.sparkfits").option("hdu",1)\
    .load("/home/plaszczy/fits/galbench_srcs_s1_0.fits")\
    .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))\
    .persist(StorageLevel.MEMORY_ONLY_SER)
# .cache()

gal.printSchema()
gal.show(10)

#get all statitics on z
gal.describe(['z']).show()

#get some base statitics on z
gal.select(F.mean(gal.z),F.min(gal.z),F.max(gal.z)).show()

#some functions as correlations are availbale form gal.stat

#add gaussian smearing
from pyspark.sql.functions import rand, randn

#tomographie
shell=gal.filter(gal['z'].between(0.1,0.2))

#come back to numpy world
