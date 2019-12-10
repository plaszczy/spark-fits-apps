
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.Row

// Logger info
import org.apache.log4j.Level
import org.apache.log4j.Logger


// Set to Level.WARN is you want verbosity
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)






//COMMON CUTS
val magcut=25.3

//GALCAT cosmodc2
var gal=spark.read.parquet(System.getenv("COSMODC2"))
val cols=Array("galaxy_id","ra","dec","mag_i","mag_r")

gal=gal.select(cols.head,cols.tail: _*).na.drop
//filter
gal=gal.filter($"mag_i"<magcut)

/*
//STARS
var stars=spark.read.format("fits").option("hdu",1).load("all_stars_DC2_run2.1i.fits")
val cols=Array("simobjid","ra","decl","rmag","imag")
stars=stars.select(cols.head,cols.tail: _*).na.drop
//cut?
*/

//OBJECT CAT
var obj=spark.read.parquet(System.getenv("RUN2"))
val cols=Array("objectId","ra","dec","psf_fwhm_i","mag_i_cModel","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i","mag_r_cModel","magerr_r_cModel","clean","snr_i_cModel","snr_r_cModel","blendedness","extendedness")

obj=obj.select(cols.head,cols.tail: _*).na.drop
//filter
obj=obj.filter($"mag_i_cModel"<magcut)



///:load scripts/cross-tools.scala
val df1=single_match(obj,gal)

//df1.write.parquet("df3.parquet")
