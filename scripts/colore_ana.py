#input
import os
f=os.environ.get("fitsdir","file:///home/plaszczy/fits/galbench_srcs_s1_0.fits")

#initialisations
from pyspark.sql import SparkSession
from pyspark import StorageLevel


spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext

#logger
logger = sc._jvm.org.apache.log4j
level = getattr(logger.Level, "WARN")
logger.LogManager.getLogger("org"). setLevel(level)
logger.LogManager.getLogger("akka").setLevel(level)

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

        
timer=Timer()
ddt=[]

#######
ana="1: load(HDU)"
from pyspark.sql import functions as F
gal=spark.read.format("com.sparkfits").option("hdu",1)\
     .load(f)\
     .select(F.col("RA"), F.col("Dec"), (F.col("Z_COSMO")+F.col("DZ_RSD")).alias("z"))
#     .cache()
#     .persist(StorageLevel.MEMORY_ONLY_SER)

gal.printSchema()
ddt.append(timer.step())
timer.print(ana)
#######
ana="2: PZ + show(5)"
from pyspark.sql.functions import randn
gal=gal.withColumn("zrec",(gal.z+0.03*(1+gal.z)*randn()).astype('float'))
#only randoms
gal.show(5)
ddt.append(timer.step())
timer.print(ana)

####
ana="3: cache (count)"
print("N={}".format(gal.cache().count()))
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
from pyspark.sql.types import IntegerType,FloatType
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

ana="8: histo (UDF)"
#binNumber=F.udf(lambda z: int((z-zmin)/dz))
#from pyspark.sql import SQLContext
#sqlContext = SQLContext.getOrCreate(sc)
#binNumber=sqlContext.udf.register("binNumber",lambda z: int((z-zmin)/dz))
#std python func
def binNumber(z):
    return int((z-zmin)/dz)
#turn to udf
binNumber_udf=F.udf(lambda x: binNumber(x))

#NOK on previous versions
#binNumber_udf=spark.udf.register("binNumber",binNumber,IntegerType())

p_udf=gal.select(gal.z,binNumber_udf(gal.z).alias('bin')).groupBy("bin").count().orderBy(F.asc("bin")).toPandas()
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


ana="10: RDD histogram"
#p_rdd=gal.select(gal.z).rdd.flatMap(list).histogram(Nbins)
p_rdd=gal.select(gal.z).rdd.map(lambda r: r.z).histogram(Nbins)
ddt.append(timer.step())
timer.print(ana)

f=open("python_perf.txt","a")
for t in ddt:
    f.write(str(t)+"\t")
f.write("\n")
f.close()

###############
#ana="tomographie"
#shell=gal.filter(gal['z'].between(0.1,0.2))
#timer.print(ana)
