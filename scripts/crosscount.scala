import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

// Logger info
import org.apache.log4j.Level
import org.apache.log4j.Logger


// Set to Level.WARN is you want verbosity
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

//Timer
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

//Healpix
class ExtPointing extends Pointing with java.io.Serializable
val nside=131072
sc.broadcast(nside)

val hp = new HealpixBase(nside, NESTED)

case class HealpixGrid(hp : HealpixBase, ptg : ExtPointing) {
  def index(theta : Double, phi : Double) : Long = {
    ptg.theta = theta
    ptg.phi = phi
    hp.ang2pix(ptg)
  }
  def neighbours(ipix:Long):Array[Long] =  {
    hp.neighbours(ipix)
  }

}

val grid = HealpixGrid(hp, new ExtPointing)
val Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))

val pix_neighbours=spark.udf.register("pix_neighbours",(ipix:Long)=>grid.neighbours(ipix))

/************************/
val timer=new Timer

//SOURCE=run2

val df_src=spark.read.parquet(System.getenv("RUN2"))

// select columns
var df=df_src.select("objectId","ra","dec","clean","mag_i_cModel","snr_i_cModel","blendedness","extendedness").na.drop

//filter
df=df.filter($"mag_i_cModel"<25.3)
//df=df.filter($"clean"===1).filter($"extendedness"===1)
//.filter($"snr_i_cModel">3)

//add theta-phi and healpixels
df=df.withColumn("theta_s",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_s",F.radians("ra"))
df=df.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra","dec")

println("*** caching source: "+df.columns.mkString(", "))
val Ns=df.cache.count
println(f"input size=${Ns/1e6}%3.2f M")


//ADD DUPLICATES
val dfn=df.withColumn("neighbours",pix_neighbours($"ipix"))

val dup=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dup(i)=dfclean1
}
dup(8)=df

val source=dup.reduceLeft(_.union(_))

println("*** caching source+duplicates: "+source.columns.mkString(", "))

val Ndup=source.cache().count

println(f"source+duplicates size=${Ndup/1e6}%3.2f M")

df.unpersist

/************************/
//TARGET=cosmodc2

val df_t=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("galaxy_id","ra","dec","mag_i").na.drop

//filter
df=df.filter($"mag_i"<25.3)

//add healpixels
df=df.withColumn("theta_t",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_t",F.radians("ra"))
df=df.withColumn("ipix",Ang2pix($"theta_t",$"phi_t")).drop("ra","dec")

val target=df

println("*** caching target: "+target.columns.mkString(", "))

val Nt=target.cache().count

println(f"target size=${Nt/1e6}%3.2f M")

///////////////////////////////////////////

//PAIRS
//join by ipix: tous les candidats paires
var matched=source.join(target,"ipix").drop("ipix")

println("joining on ipix: "+matched.columns.mkString(", "))
val nmatch=matched.cache.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")

//release mem
source.unpersist
target.unpersist


val cands=matched.groupBy("objectId").count
val nc=cands.count

val single=cands.filter($"count"===F.lit(1))
val n_out1=single.count


println(f"|${Nt/1e6}%3.2f | ${Ns/1e6}%3.2f | ${nc/1e6}%3.2f (${nc.toFloat/Ns*100}%.1f%%) | ${n_out1/1e6}%3.2f (${n_out1.toFloat/Ns*100}%.1f%%)|")

val dt=timer.step
timer.print("completed")
