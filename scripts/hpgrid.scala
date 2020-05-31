import healpix.essentials.HealpixBase
import healpix.essentials.Pointing
import healpix.essentials.Scheme.{NESTED,RING}


//Healpix
class ExtPointing extends Pointing with java.io.Serializable
//val hp = new HealpixBase(nside, NESTED)
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
/*
val grid = HealpixGrid(new HealpixBase(nside, NESTED), new ExtPointing)
val Ang2pix=spark.udf.register("Ang2pix",(theta:Double,phi:Double)=>grid.index(theta,phi))
val pix_neighbours=spark.udf.register("pix_neighbours",(ipix:Long)=>grid.neighbours(ipix))
 */