import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

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
var df=df_src.select("objectId","ra","dec","mag_i","psf_fwhm_i")

//append _s but on id
for (n <- df.columns.tail) df=df.withColumnRenamed(n,n+"_s")

//filter
df=df.filter($"mag_i_s"<25.3).drop("mag_i_s")

//add theta-phi and healpixels
df=df.withColumn("theta_s",F.radians(F.lit(90)-F.col("dec_s"))).withColumn("phi_s",F.radians("ra_s"))
df=df.withColumn("ipix",Ang2pix($"theta_s",$"phi_s")).drop("ra_s","dec_s")

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

source.printSchema
println("caching source+duplicates")

val Ns=source.cache().count

println(f"source size=${Ns/1e6}%3.2f M")


/************************/
//TARGET=cosmodc2

val df_t=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("galaxy_id","ra","dec","mag_i")
//append _t
for (n <- df.columns) df=df.withColumnRenamed(n,n+"_t")

//filter
df=df.filter($"mag_i_t"<25.3).drop("mag_i_t")

//add healpixels
df=df.withColumn("theta_t",F.radians(F.lit(90)-F.col("dec_t"))).withColumn("phi_t",F.radians("ra_t"))
df=df.withColumn("ipix",Ang2pix($"theta_t",$"phi_t")).drop("ra_t","dec_t")

val target=df

println("caching target")

val Nt=target.cache().count

println(f"target size=${Nt/1e6}%3.2f M")

///////////////////////////////////////////

//PAIRS
//join by ipix: tous les candidats paires
var matched=source.join(target,"ipix").drop("ipix")

println("joining on ipix")

val nmatch=matched.cache.count()
println(f"matched size=${nmatch/1e6}%3.2f M")

//add euclidian distance
//matched=matched.withColumn("d",F.hypot(matched("phi_t")-matched("phi_s"),F.sin((matched("theta_t")+matched("theta_s"))/2)*(matched("theta_t")-matched("theta_s"))))

matched=matched.withColumn("dx",F.sin($"theta_t")*F.cos($"phi_t")-F.sin($"theta_s")*F.cos($"phi_s")).withColumn("dy",F.sin($"theta_t")*F.sin($"phi_t")-F.sin($"theta_s")*F.sin($"phi_s")).withColumn("dz",F.cos($"theta_t")-F.cos($"theta_s")).withColumn("r",F.sqrt($"dx"*$"dx"+$"dy"*$"dy"+$"dz"*$"dz")).drop("dx","dy","dz","theta_t","theta_s","phi_t","phi_s","mag_i_s","mag_i_t")


// combien de candidats par source
//val nass=matched.groupBy("objectId").count.withColumnRenamed("count","ncand")
//stat nass
//nass.groupBy("ncand").count.withColumn("frac",$"count"/nmatch).sort("ncand").show


// pair RDD
//create a map to retrieve position
val idx=matched.columns.map(s=>(s,matched.columns.indexOf(s))).toMap

//build paiRDD based on key objectId
val rdd=matched.rdd.map(r=>(r.getLong(idx("objectId")),r))


def bestmatch(t:Iterable[Row]):Row = {
//attention c'est pas un iterator mais Iterable donc Traversable (foreach)

  val ir:Int=idx("r")
  val rbest=t.reduceLeft((r1,r2) => if (r1.getDouble(ir)<r2.getDouble(ir)) r1 else r2)

  Row(rbest.getLong(idx("galaxy_id")),rbest.getDouble(idx("r")),rbest.getDouble(idx("psf_fwhm_i_s")),t.size)
}

println("grouping by source id")
//groupby objectId : apply bestchoice
val assoc=rdd.groupByKey.mapValues(bestmatch)

//faire un df
val perfect=assoc.map(x=>(x._1,x._2.getLong(0),x._2.getDouble(1),x._2.getDouble(2),x._2.getInt(3))).toDF("objectId","galaxy_id","dr","fwhm","nass")
perfect.show

// check fwhm
val best=perfect.filter($"nass"===F.lit(1)).withColumn("sigma_r",F.radians($"fwhm"/2.355/3600)).withColumn("cum",F.lit(1.0)-F.exp(-$"dr"*$"dr"/($"sigma_r"*$"sigma_r"*2.0)))

