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
}

val grid = HealpixGrid(hp, new ExtPointing)
val Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))


//read DF
val df_all=spark.read.parquet("run2_extrait.parquet")

//add theta/phi
val df=df_all.withColumn("theta",F.radians(F.lit(90)-F.col("dec"))).withColumn("phi",F.radians("ra"))


val dfpix=df.withColumn("ipix",Ang2pix($"theta",$"phi")).drop("theta","phi")

println(dfpix.cache().count)
