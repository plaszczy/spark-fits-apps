import org.apache.spark.sql.{functions=>F}

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer



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


//SOURCE
val df_all=spark.read.parquet(System.getenv("RUN2")).select($"ra".as("ra_s"),$"dec".as("dec_s"),$"mag_i".as("mag_i_s"))
//val df_all=spark.read.parquet("run2_extrait.parquet")


//filter
val gold=df_all.filter($"mag_i_s"<25.3).withColumn("id_s",F.monotonically_increasing_id)


//add healpixels
val df=gold.withColumn("theta",F.radians(F.lit(90)-F.col("dec_s"))).withColumn("phi",F.radians("ra_s"))
//ang2pix
var source=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")


//add neighbours
val dfpixn=source.withColumn("neighbours",pix_neighbours($"ipix"))
//create duplicates and add to source
for (i <- 0 to 7) {
  println(i)
  val df1=dfpixn.drop("ipix").withColumn("ipix",$"neighbours"(i))
  val dfclean1=df1.filter(not(df1("ipix")===F.lit(-1))).drop("neighbours")
  source=source.union(dfclean1)
}

//TARGET
println("source=",source.cache().count)
