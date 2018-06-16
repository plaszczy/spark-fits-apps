import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer

import java.io._

val f="/home/plaszczy/fits/galbench_srcs_s1_0.fits"
//val f="hdfs://134.158.75.222:8020//lsst/LSST10Y"

//timer
class Timer (var t0:Double=System.nanoTime().toDouble,   var dt:Double=0)  {
  def step:Double={
    val t1 = System.nanoTime().toDouble
    dt=(t1-t0)/1e9
    t0=t1
    dt
  }
  def print(msg:String):Unit={
    val sep="----------------------------------------"
    println("\n"+msg+": "+dt+" s\n"+sep)
  }
}


val timer=new Timer
var ddt=ArrayBuffer[Double]()

var ana="load(HDU)"
val galraw=spark.read.format("com.sparkfits").option("hdu",1).load(f).select($"RA",$"Dec",($"Z_COSMO"+$"DZ_RSD").as("z"))

galraw.printSchema
ddt+=timer.step
timer.print(ana)


ana="PZ + show(5)"
val gal=galraw.withColumn("zrec",(galraw("z")+lit(0.03)*(lit(1)+galraw("z"))*randn()).cast(FloatType))
gal.show(5)
ddt+=timer.step
timer.print(ana)

//cache
//gal.repartition(10000).rdd.getNumPartitions

ana="cache (count)"
val N=gal.cache.count
println("Ntot="+N)
ddt+=timer.step
timer.print(ana)

//stat
ana="statistics z"
gal.describe("z").show()
ddt+=timer.step
timer.print(ana)

//ana="statistics all"
gal.describe().show
ddt+=timer.step
timer.print(ana)

ana="minmax"
val minmax=gal.select(min("z"),max("z")).first()
val zmin=minmax(0).toString.toDouble
val zmax=minmax(1).toString.toDouble
val Nbins=100
val dz=(zmax-zmin)/Nbins
ddt+=timer.step
timer.print(ana)

ana="histo df"
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect
ddt+=timer.step
timer.print(ana)

ana="histo UDF"
val sqlContext = SQLContext.getOrCreate(sc)
val binNumber=sqlContext.udf.register("binNumber",(z:Float)=>((z-zmin)/dz).toInt)
val zbin=gal.select($"z",(binNumber(gal("z"))).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect

ddt+=timer.step
timer.print(ana)

//histoRDD
//ana="histo RDD countbykey"
//val h=zbin.select("bin").rdd.map(r=>(r(0),1)).countByKey()
// manque le sort dans scala car Map[T, Long]


ana="histo RDD "
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.select("bin").rdd.map(r=>(r(0),1)).reduceByKey(_+_).sortBy(c => c._2, true)
//.map(x=>(zmin+dz/2+x._1*dz,x._2)
h.collect

ddt+=timer.step
timer.print(ana)

ana="RDD histogram"
//save to csv
//p.repartition(1).write.csv("histo.csv")
//p.write.json("histo.json")
//p.rdd.repartition(1).saveAsTextFile("histo.txt")

//java
val file = "scala_perf.txt"
val append=true
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append))) 
for (x <- ddt) writer.write(x.toString+"\t")
writer.write("\n")
writer.close
