
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


val obj_cols=Array("objectId","ra","dec","mag_i_cModel","psf_fwhm_i","magerr_i_cModel","cModelFlux_i","cModelFluxErr_i","clean","snr_i_cModel","blendedness","extendedness")

val cosmo_cols=Array("galaxy_id","ra","dec","mag_i")

val star_cols=Array("simobjid","ra","decl","rmag","imag"


//SOURCE=run2
val obj=spark.read.parquet(System.getenv("RUN2"))

val stars=spark.read.format("fits").option("hdu",1).load("all_stars_DC2_run2.1i.fits")

// select columns
var df=src.select(cols).na.drop

//filter
df=df.filter($"mag_i_cModel"<magcut)


/************************/
//TARGET=cosmodc2

val gal=spark.read.parquet(System.getenv("COSMODC2"))
// select columns
df=df_t.select("galaxy_id","ra","dec","mag_i").na.drop

//filter
df=df.filter($"mag_i"<magcut)
