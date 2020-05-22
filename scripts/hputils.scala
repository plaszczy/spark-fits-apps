import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.NESTED

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.math.{log,toRadians,pow,floor}

//args from --conf spark.driver.args="10"
//compute nside from sep=args(0)
val args = sc.getConf.get("spark.driver.args").split("\\s+")
val sepcut=args(0).toDouble

val L=toRadians(sepcut/60)
val i=floor(-log(L)/log(2.0)).toInt
val nside=pow(2,i-1).toInt

println(s"sep=$sepcut arcmin -> nside=$nside")

//Healpix
class ExtPointing extends Pointing with java.io.Serializable
//val nside=262144
//val nside=131072
//val nside=32


print(s"Using NSIDE=$nside")


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
