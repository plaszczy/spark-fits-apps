
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

  def time:Double=System.nanoTime().toDouble

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
val nside=262144
#val nside=131072
//val nside=65536

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
val start=timer.time

//SOURCE=run2

val magcut=25.3
val snrcut=1.0

val df_src=spark.read.parquet(System.getenv("RUN2"))

// select columns
var df=df_src.select("objectId","ra","dec","mag_i_cModel","psf_fwhm_i","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i","clean","snr_i_cModel","blendedness","extendedness").na.drop

//filter
df=df.filter($"mag_i_cModel"<magcut).filter($"snr_i_cModel">snrcut)

//add theta-phi and healpixels
df=df.withColumn("theta_s",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_s",F.radians("ra"))
val source=df.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra","dec")

println("*** caching source: "+df.columns.mkString(", "))
val Ns=source.cache.count
println(f"input size=${Ns/1e6}%3.2f M")

timer.step
timer.print("source cache")

//ADD DUPLICATES
val dfn=source.withColumn("neighbours",pix_neighbours($"ipix"))

val dups=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dups(i)=dfclean1
}
dups(8)=source

val dup=dups.reduceLeft(_.union(_))

println("*** caching source+duplicates: "+dup.columns.mkString(", "))

val Ndup=source.cache().count

println(f"source+duplicates size=${Ndup/1e6}%3.2f M")

timer.step
timer.print("duplicates cache")

/************************/
//TARGET=cosmodc2

val df_t=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("galaxy_id","ra","dec","mag_i").na.drop

//filter
df=df.filter($"mag_i"<magcut)

//add healpixels
df=df.withColumn("theta_t",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_t",F.radians("ra"))
df=df.withColumn("ipix",Ang2pix($"theta_t",$"phi_t")).drop("ra","dec")

val target=df

println("*** caching target: "+target.columns.mkString(", "))
val Nt=target.cache().count
println(f"target size=${Nt/1e6}%3.2f M")

timer.step
timer.print("target cache")

///////////////////////////////////////////
//PAIRS
//join by ipix: tous les candidats paires
var matched=dup.join(target,"ipix").drop("ipix")

println("==> joining on ipix: "+matched.columns.mkString(", "))
val nmatch=matched.cache.count()
println(f"#pair-associations=${nmatch/1e6}%3.2f M")

//release mem
dup.unpersist
target.unpersist

timer.step
timer.print("join")




//add euclidian distance
//matched=matched.withColumn("d",F.hypot(matched("phi_t")-matched("phi_s"),F.sin((matched("theta_t")+matched("theta_s"))/2)*(matched("theta_t")-matched("theta_s"))))

//add distance column
matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("d",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).drop("dx","dy","dz")

// pair RDD
//create a map to retrieve position
val idx=matched.columns.map(s=>(s,matched.columns.indexOf(s))).toMap

//build paiRDD based on key objectId

/*
val rdd=matched.rdd.map(r=>(r.getLong(idx("objectId")),r))
val ir:Int=idx("d")
val ass_rdd=rdd.reduceByKey((r1,r2)=> if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2)
val ass=spark.createDataFrame(ass_rdd.map(x=>x._2),matched.schema)
 */

//reduce by min(r) and accumlate counts
val ir:Int=idx("d")
val rdd=matched.rdd.map{ r=>(r.getLong(idx("objectId")),(r,1L)) } 
val ass_rdd=rdd.reduceByKey { case ( (r1,c1),(r2,c2)) => (if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2 ,c1+c2) }
val ass=spark.createDataFrame(ass_rdd.map{ case (k,(r,c))=> Row.fromSeq(r.toSeq++Seq(c)) },matched.schema.add(StructField("nass",LongType,true)))

println("*** reducebykey id_source")
val nc=ass.cache.count

timer.step
timer.print("reducebykey")


matched.unpersist

//stat on # associations: unnecessary
ass.groupBy("nass").count.withColumn("frac",$"count"/nc).sort("nass").show


// ncand=1
var df1=ass.filter($"nass"===F.lit(1)).withColumn("r",F.degrees($"d")*3600).drop("nass","d")

println("*** caching df1: "+df1.columns.mkString(", "))
val nout1=df1.cache.count
df1.printSchema

println(f"||i<${magcut}|| ${Ns/1e6}%3.2f || ${nc/1e6}%3.2f (${nc.toFloat/Ns*100}%.1f%%) || ${nout1/1e6}%3.2f (${nout1.toFloat/nc*100}%.1f%%)||")

timer.step
timer.print("completed")

val stop=timer.time
println(f"TOT TIME=${stop-start}")

//extra cuts
/*
df1=df1.withColumn("flux_i",F.pow(10.0,-($"mag_i"-31.4)/2.5))
df1=df1.filter(F.abs(($"cModelFlux_i"-$"flux_i")/$"cModelFluxErr_i")<3)
 */


/*
//assoc statitics
var df_src=source.filter( ($"clean"===1) && ($"extendedness"===1))
var df_ass=ass.filter( ($"clean"===1) && ($"extendedness"===1))

var N=df_src.count
var Na=df_ass.count
var N1=df_ass.filter($"nass"===F.lit(1)).count

println(f"|| clean+extendedness || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")

df_src=df_src.filter($"snr_i_cModel">5)
df_ass=df_ass.filter($"snr_i_cModel">5)
N=df_src.count
Na=df_ass.count
N1=df_ass.filter($"nass"===F.lit(1)).count
println(f"|| SNR>5 || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")

df_src=df_src.filter($"snr_i_cModel">10)
df_ass=df_ass.filter($"snr_i_cModel">10)
N=df_src.count
Na=df_ass.count
N1=df_ass.filter($"nass"===F.lit(1)).count
println(f"|| SNR>10 || ${N/1e6}%3.2f || ${Na/1e6}%3.2f (${Na.toFloat/N*100}%.1f%%) || ${N1/1e6}%3.2f (${N1.toFloat/Na*100}%.1f%%)||")
 */
