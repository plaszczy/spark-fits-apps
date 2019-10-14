import org.apache.spark.sql.{functions=>F}

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf


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

//val cols=List("ra","dec","mag_i")
//val c=cols.map(c=> F.col(c).as(c+"_s"))
//df_all=df_all.select(c: _*)

//add id
df=df.withColumn("id_s",F.monotonically_increasing_id)

//filter
df=df.filter($"mag_i_s"<25.3)


//add healpixels
df=df.withColumn("theta",F.radians(F.lit(90)-F.col("dec_s"))).withColumn("phi",F.radians("ra_s"))
val source=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")

val Ns=source.cache().count

println(f"source size=${Ns/1e6}%3.2f M")


/************************/
//TARGET=cosmodc2

val df_t=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("ra","dec","mag_i","redshift")

//append _t
for (n <- df.columns) df=df.withColumnRenamed(n,n+"_t")

//val cols=List("ra","dec","mag_i")
//val c=cols.map(c=> F.col(c).as(c+"_s"))
//df_all=df_all.select(c: _*)

//add id
df=df.withColumn("id_t",F.monotonically_increasing_id)

//filter
df=df.filter($"mag_i_t"<25.3)


//add healpixels
df=df.withColumn("theta",F.radians(F.lit(90)-F.col("dec_t"))).withColumn("phi",F.radians("ra_t"))
df=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")



//add neighbours
val dfn=df.withColumn("neighbours",pix_neighbours($"ipix"))

//create duplicates
val dup=new Array[org.apache.spark.sql.DataFrame](9)

for (i <- 0 to 7) {
  println(i)
  val df1=dfn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  dup(i)=dfclean1
}

dup(8)=df

val target=dup.reduceLeft(_.union(_))

val Nt=target.cache().count

//TARGET
println(f"target size=${Nt/1e6}%3.2f M")


//join by index: tous les candidats paires
val matched=source.join(target,"ipix").drop("ipix")
println(f"matched size=${matched.cache.count()/1e6}%3.2f M")


//val rdd=matched.drop("ipix")

//choix par source
// combien de candidats a chaque fois
val nass=matched.groupBy("id_s").count.withColumnRenamed("count","ncand")
//stat nass
nass.groupBy("ncand").count.sort("ncand").show


//df : agg ou window?


// pair RDD
//find location of id_s
val id=matched.columns.indexOf("id_s")
val rdd=matched.rdd
val rdd=matched.rdd.map(r=>(r.getLong(id),r))


def bestcand(it:Iterable[org.apache.spark.sql.Row]):org.apache.spark.sql.Row = it.head


val assoc=rdd.groupByKey.mapValues(bestcand)
