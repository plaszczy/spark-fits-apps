import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._

val f="/home/plaszczy/fits/galbench_srcs_s1_0.fits"

//time
class Timer {
  def print(msg:String){
    val t1 = System.nanoTime().toDouble
    val dt=(t1-t0)/1e9
    println("--------------------------------")
    println(msg+" > Elapsed time:" + " " + dt + "s" )
    t0=t1
  }
  var t0:Double=System.nanoTime().toDouble
}

val timer=new Timer

var ana="load(HDU)"
val galraw=spark.read.format("com.sparkfits").option("hdu",1).load(f).select($"RA",$"Dec",($"Z_COSMO"+$"DZ_RSD").as("z"))

galraw.printSchema
timer.print(ana)


ana="PZ + show(5)"
val gal=galraw.withColumn("zrec",(galraw("z")+lit(0.03)*(lit(1)+galraw("z"))*randn()).cast(FloatType))
gal.show(5)
timer.print(ana)

//cache
ana="cache (count)"
val N=gal.cache.count
println("Ntot="+N)
timer.print(ana)

//stat
ana="statistics z"
gal.describe("z").show()
timer.print(ana)

//ana="statistics all"
gal.describe().show
timer.print(ana)

ana="minmax"
val minmax=gal.select(min("z"),max("z")).first()
val zmin=minmax(0).toString.toDouble
val zmax=minmax(1).toString.toDouble
val Nbins=100
val dz=(zmax-zmin)/Nbins
timer.print(ana)

ana="histo"
val zbin=gal.select($"z",(((gal("z")-lit(zmin+dz/2)))/dz).cast(IntegerType).as("bin"))
val h=zbin.groupBy("bin").count.sort("bin")
val p=h.select($"bin",(lit(zmin+dz/2)+h("bin")*lit(dz)).as("zbin"),$"count").drop("bin")
val c=p.collect
timer.print(ana)
//save to csv
//p.repartition(1).write.csv("histo.csv")
//p.write.json("histo.json")
//p.rdd.repartition(1).saveAsTextFile("histo.txt")
//val file = "whatever.txt"
//val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file))) 
//for (x <- c) {
//  writer.write(x(0).toString+"\t"+x(1).toString+"\n")
//}
//writer.close

ana="histo RDD countbykey"
val h=zbin.select("bin").rdd.map(r=>(r(0),1)).countByKey()

ana="histo RDD reducebykey"
val h=zbin.select("bin").rdd.map(r=>(r(0),1)).reduceByKey(_+_)
timer.print(ana)

ana="RDD histogram"
