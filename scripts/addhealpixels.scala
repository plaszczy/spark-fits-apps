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
var df=df_src.select("ra","dec","mag_i")
//append _s
for (n <- df.columns) df=df.withColumnRenamed(n,n+"_s")

//filter
df=df.filter($"mag_i_s"<25.3)

//add id
df=df.withColumn("id_s",F.monotonically_increasing_id)

//add healpixels
df=df.withColumn("theta",F.radians(F.lit(90)-F.col("dec_s"))).withColumn("phi",F.radians("ra_s"))
df=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")

//DUPLICATES
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

val Ns=source.cache().count

println(f"source size=${Ns/1e6}%3.2f M")


/************************/
//TARGET=cosmodc2

val df_t=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("ra","dec","mag_i","redshift")
//append _t
for (n <- df.columns) df=df.withColumnRenamed(n,n+"_t")

//filter
df=df.filter($"mag_i_t"<25.3)

//add id
df=df.withColumn("id_t",F.monotonically_increasing_id)

//add healpixels
df=df.withColumn("theta",F.radians(F.lit(90)-F.col("dec_t"))).withColumn("phi",F.radians("ra_t"))
df=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")

/*
//DUPLICATES
val dfn=df.withColumn("neighbours",pix_neighbours($"ipix"))

val dup=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dup(i)=dfclean1
}

dup(8)=df

val target=dup.reduceLeft(_.union(_))
 */
val target=df
val Nt=target.cache().count

//TARGET
println(f"target size=${Nt/1e6}%3.2f M")


//join by index: tous les candidats paires
val matched=source.join(target,"ipix").drop("ipix")
val nmatch=matched.cache.count()
println(f"matched size=${nmatch/1e6}%3.2f M")


// combien de candidats par source
val nass=matched.groupBy("id_s").count.withColumnRenamed("count","ncand")
//stat nass
nass.groupBy("ncand").count.withColumn("frac",$"count"/nmatch).sort("ncand").show


//df : agg ou window?


// pair RDD
//create a map to retrieve position
val idx=matched.columns.map(s=>(s,matched.columns.indexOf(s))).toMap

//build paiRDD based on key id_s
val rdd=matched.rdd.map(r=>(r.getLong(idx("id_s")),r))


def bestmatch(it:Iterable[Row]):Row = {
  val Ncand=it.size
  var id_t:Long = -1
  var d:Double = -1.0
  for (r <- it) {
    id_t= r.getLong(idx("id_t"))
    val ra1= r.getFloat(idx("ra_s"))
    val ra2= r.getFloat(idx("ra_t"))
    val dec1= r.getFloat(idx("dec_s"))
    val dec2= r.getFloat(idx("dec_t"))
  }
  Row(id_t,Ncand)
}

val assoc=rdd.groupByKey.mapValues(bestmatch)

//verif
//assoc.map(r=>r._2(9).asInstanceOf[Int]).map((_,1)).reduceByKey(_+_).take(10)

//faire un df
assoc.map(x=>(x._1,x._2.getLong(0),x._2.getInt(1))).toDF
