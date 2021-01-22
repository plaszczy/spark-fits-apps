import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

val f= scala.util.Properties.envOrElse("SKYSIM","")
print(f)

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

import spark.implicits._

val timer=new Timer
var ddt=ArrayBuffer[Double]()

var ana="1: load(HDU)"

val galraw=spark.read.parquet(f).select(col("ra"),col("dec"),col("mag_i"),col("redshift").as("z"))
galraw.printSchema
ddt+=timer.step
timer.print(ana)

//cache
val gal=galraw.coalesce(10000)
print(gal.rdd.getNumPartitions)

ana="3: cache (count)"
val N=gal.cache.count
ddt+=timer.step
timer.print(ana)

//stat
ana="4: statistics z"
gal.describe("z").show()
ddt+=timer.step
timer.print(ana)

ana="5: statistics all"
gal.describe().show
ddt+=timer.step
timer.print(ana)

ana="6: minmax"
val minmax=gal.select(min("z"),max("z")).first()
val zmin=minmax.getFloat(0)
val zmax=minmax.getFloat(1)
val Nbins=100
val dz=(zmax-zmin)/Nbins
ddt+=timer.step
timer.print(ana)

ana="7: histo df"
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect
ddt+=timer.step
timer.print(ana)

ana="8a: histo UDF"
val sqlContext = SQLContext.getOrCreate(sc)
//val binNumber=sqlContext.udf.register("binNumber",(z:Float)=>((z-zmin)/dz).toInt)
val binNumber=spark.udf.register("binNumber",(z:Float)=>((z-zmin)/dz).toInt)
val zbin=gal.select($"z",(binNumber(gal("z"))).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect
ddt+=timer.step
timer.print(ana)




//java write
import java.io._
val file = "scala_perf.txt"
val append=true
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append))) 
for (x <- ddt) writer.write(x.toString+"\t")
writer.write("\n")
writer.close
