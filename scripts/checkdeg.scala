//spark-shell --jars jhealpix.jar -i hpgrid.scala -i Timer.scala
//:load hpgrid.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row


import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.math.{log,toRadians,pow,floor,sin}

val sepcut=10.0
val thetacut=toRadians(sepcut/60)
val rcut=2*sin(thetacut/2)
val rcut2=rcut*rcut


//nside
val L=toRadians(sepcut/60)
val i=floor(-log(L)/log(2.0)).toInt
val nside=pow(2,i-1).toInt

//healpix
val grid = HealpixGrid(new HealpixBase(nside, NESTED), new ExtPointing)
def Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))
def pix_neighbours=spark.udf.register("pix_neighbours",(ipix:Long)=>grid.neighbours(ipix))


val timer=new Timer
val start=timer.time


//SOURCE
val input=spark.read.parquet("deg.parquet")
//add id+ipix
val source=input.withColumn("id",F.monotonicallyIncreasingId).withColumn("theta_s",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_s",F.radians("ra")).withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra","dec").cache()

println("*** caching source: "+source.columns.mkString(", "))
val Ns=source.count
println(f"input size=${Ns/1e6}%3.2f M")
timer.step
timer.print("source cache")

val np1=source.rdd.getNumPartitions
println("source partitions="+np1)

//DUPLICATES
val dfn=source.withColumn("neighbours",pix_neighbours($"ipix"))

val dups=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dups(i)=dfclean1
}
dups(8)=source

val dup=dups.reduceLeft(_.union(_)).withColumnRenamed("id","id2").withColumnRenamed("theta_s","theta_t").withColumnRenamed("phi_s","phi_t").cache

println("*** caching duplicates: "+dup.columns.mkString(", "))
val Ndup=dup.count
println(f"duplicates size=${Ndup/1e6}%3.2f M")

timer.step
val np2=dup.rdd.getNumPartitions
println("dup partitions="+np2)


///////////////////////////////////////////
//PAIRS
val pairs=source.join(dup,"ipix").drop("ipix").filter('id=!='id2)
//.persist(StorageLevel.MEMORY_AND_DISK)
//println("pairs size="+pairs.count)

//spherical distance
val edges=pairs.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("r2",$"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz").drop("dx","dy","dz","theta_t","theta_s","phi_s","phi_t").filter($"r2"<rcut2).drop("r2")



println("==> joining on ipix: "+edges.columns.mkString(", "))
val nmatch=edges.cache.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")

//release mem
dup.unpersist

timer.step
timer.print("join")

//only non 0 degrees
val deg=edges.groupBy("id").count
println(deg.cache.count)

timer.step
timer.print("groupBy")

val fulltime=(timer.time-start)*1e-9/60
println(f"TOT TIME=${fulltime}")

//val df=source.drop("theta_s","phi_s","ipix").join(deg,"id")
val df=deg.join(source.drop("theta_s","phi_s","ipix"),"id")
df.withColumn("diff",$"count"-$"degtrue").describe("diff").show


val sumDegIn=source.agg(F.sum("degtrue")).first.getLong(0)
val sumDegOut=deg.agg(F.sum("count")).first.getLong(0)
//attention meandegree=sumDegOut.toFloat/Ns
println(s"Sum degree=$sumDegIn vs. $sumDegOut vs $nmatch")
require(sumDegOut==sumDegIn)
