import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.SQLContext

import java.io._

val f="/home/plaszczy/fits/galbench_srcs_s1_0.fits"
//val f="hdfs://134.158.75.222:8020//lsst/LSST10Y"

//timer
class Timer (var t0:Double=System.nanoTime().toDouble,   var dt:Double=0)  {
  def step(){
    val t1 = System.nanoTime().toDouble
    dt=(t1-t0)/1e9
    t0=t1
  }
}


val timer=new Timer
//var ddt=Array[Double]()

var ana="load(HDU)"
val galraw=spark.read.format("com.sparkfits").option("hdu",1).load(f).select($"RA",$"Dec",($"Z_COSMO"+$"DZ_RSD").as("z"))

galraw.printSchema
val sep="----------------------------------------"
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

ana="PZ + show(5)"
val gal=galraw.withColumn("zrec",(galraw("z")+lit(0.03)*(lit(1)+galraw("z"))*randn()).cast(FloatType))
gal.show(5)
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

//cache
//gal.repartition(10000).rdd.getNumPartitions

ana="cache (count)"
val N=gal.cache.count
println("Ntot="+N)
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

//stat
ana="statistics z"
gal.describe("z").show()
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

//ana="statistics all"
gal.describe().show
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

ana="minmax"
val minmax=gal.select(min("z"),max("z")).first()
val zmin=minmax(0).toString.toDouble
val zmax=minmax(1).toString.toDouble
val Nbins=100
val dz=(zmax-zmin)/Nbins
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

ana="histo df"
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect
timer.step
println(ana+timer.dt+" s"+"\n"+sep)

ana="histo UDF"
val sqlContext = SQLContext.getOrCreate(sc)
val binNumber=sqlContext.udf.register("binNumber",(z:Float)=>((z-zmin)/dz).toInt)
val zbin=gal.select($"z",(binNumber(gal("z"))).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect

timer.step
println(ana+timer.dt+" s"+"\n"+sep)

//histoRDD
//ana="histo RDD countbykey"
//val h=zbin.select("bin").rdd.map(r=>(r(0),1)).countByKey()
// manque le sort dans scala car Map[T, Long]


ana="histo RDD "
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.select("bin").rdd.map(r=>(r(0),1)).reduceByKey(_+_).sortBy(c => c._2, true)
//.map(x=>(zmin+dz/2+x._1*dz,x._2)
h.collect

timer.step
println(ana+timer.dt+" s"+"\n"+sep)

ana="RDD histogram"

//save to csv
//p.repartition(1).write.csv("histo.csv")
//p.write.json("histo.json")
//p.rdd.repartition(1).saveAsTextFile("histo.txt")

//java
val file = "scala_perf.txt"
val append=true
//val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append))) 
//for (x <- c) {
//  writer.write(x(0).toString+"\t"+x(1).toString+"\n")
//}
//writer.close
