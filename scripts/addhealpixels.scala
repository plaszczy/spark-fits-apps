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


class ExtPointing extends Pointing with java.io.Serializable
case class Point2D(ra: Double, dec: Double)

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
//SOURCE=run2

val df_src=spark.read.parquet(System.getenv("RUN2"))

// select columns
var df=df_src.select("objectId","ra","dec","mag_i_cModel","psf_fwhm_i","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i").na.drop

//filter
df=df.filter($"mag_i_cModel"<25.3)


//add theta-phi and healpixels
df=df.withColumn("theta_s",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi_s",F.radians("ra"))
df=df.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra","dec")

println("*** caching source: "+df.columns.mkString(", "))
val n_in=df.cache.count
println(f"input size=${n_in/1e6}%3.2f M")


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

val Ns=source.cache().count

println(f"source+duplicates size=${Ns/1e6}%3.2f M")

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


matched.unpersist

//join with number of ass
/*
// combien de candidats par source
val cands=matched.groupBy("objectId").count.withColumnRenamed("count","ncand")

//stat ass
println("caching #associations")
var cand_stat=cands.cache.groupBy("ncand").count
val nc=cand_stat.rdd.map(r => r.getLong(1)).reduce(_+_)

cand_stat.withColumn("frac",$"count"/nc).sort("ncand").show

println("join with ncands (caching)")
val assoc=ass.cache.join(cands,"objectId")
 */


//stat on # associations: unnecessary
ass.groupBy("nass").count.withColumn("frac",$"count"/nc).sort("nass").show



// ncand=1
val df1=ass.filter($"nass"===F.lit(1)).withColumn("r",F.degrees($"d")*3600).withColumn("sigr",$"psf_fwhm_i"/2.355).withColumn("cumr",F.lit(1.0)-F.exp(-$"r"*$"r"/($"sigr"*$"sigr"*2.0))).drop("nass","d","psf_fwhm_i")

println("*** caching df1: "+df1.columns.mkString(", "))
val n_out1=df1.cache.count
df1.printSchema

println(f"IN=${n_in/1e6}%3.2f M MATCHED=${nc/1e6}%3.2f M (${nc.toFloat/n_in*100}%.1f%%) GOLD=${n_out1/1e6}%3.2f M (${n_out1.toFloat/n_in*100}%.1f%%)")

