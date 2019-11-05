
:load df_tools.scala

var df1=spark.read.parquet("/lsst/DC2/df1.parquet")

df1=df1.withColumn("flux_i",F.pow(10.0,-($"mag_i"-31.4)/2.5)).withColumn("dflux",$"cModelFlux_i"-$"flux_i").withColumn("sigpos",$"sigr"/$"snr_i_cModel").drop("sigr").withColumn("dphi",F.degrees(F.sin(($"theta_s"+$"theta_t")/2)*($"phi_s"-$"phi_t"))*3600)

df1.cache.count

//quality cuts

df1=df1.filter( ($"clean"===1) && ($"extendedness"===1))
df1=df1.filter( ($"r"<1)
df1=df1.filter( $"snr_i_cModel">1)
df1=df1.filter($"r"/$"sigpos"<15)


//dtheta=dDEC
df_hist(df1.withColumn("ddec",F.degrees($"theta_s"-$"theta_t")*3600),"ddec",Some(-5.0,5.0),Nbins=1001,fn="ddec.txt")
df_hist(df1.withColumn("pulldec",F.degrees($"theta_s"-$"theta_t")*3600/$"sigpos"),"pulldec",Some(-15.0,15.0),Nbins=1001,fn="pulldec.txt")

//dphi = cos(dec) dRA
df_hist(df1,"dphi",Some(-5.0,5.0),Nbins=1001,fn="dcra.txt")
df_hist(df1.withColumn("dphipull",$"dphi"/$"sigpos"),"dphipull",Some(-15.0,15.0),Nbins=1001,fn="pullcra.txt")

//r
df_hist(df1,"r",Some(0,7),Nbins=1001,fn="r.txt")
df_hist(df1.withColumn("rnorm",$"r"/$"sigpos"),"rnorm",Some(0,25),Nbins=1001,fn="pullr.txt")
df_hist(df1,"cumr",Some(0,1),Nbins=1001,fn="cumr.txt")

//mags
df_hist(df1.withColumn("dmag",$"mag_i_cModel"-$"mag_i"),"dmag",Some(-15,10),Nbins=1001,fn="dmag.txt")
df_hist(df1.withColumn("pullmag",($"mag_i_cModel"-$"mag_i")/$"magerr_i_cModel"),"pullmag",Some(-300,150),Nbins=1001,fn="pullmag.txt")

//flux
df_hist(df1,"dflux",Some(-50000,50000),Nbins=1001,fn="dflux.txt")
df_hist(df1.withColumn("pullf",$"dflux"/$"cModelFluxErr_i"),"pullf",Some(-50,50),Nbins=1001,fn="pullflux.txt")
