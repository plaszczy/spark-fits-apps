
:load df_tools.scala


var df1=spark.read.parquet("/lsst/DC2/df1.parquet")

df1=df1.withColumn("flux_i",F.pow(10.0,-($"mag_i"-31.4)/2.5)).withColumn("dflux",$"cModelFlux_i"-$"flux_i").withColumn("sigpos",$"sigr"/$"snr_i_cModel").drop("sigr")
df1.cache.count

//quality cuts

df1=df1.filter( ($"clean"===1) && ($"extendedness"===1))
df1=df1.filter( ($"r"<1)
df1=df1.filter( $"snr_i_cModel">5)
df1=df1.filter($"r"/$"sigpos"<15)

//dphi
df_hist(df1.withColumn("dphi",F.degrees($"phi_s"-$"phi_t")*3600),"dphi",Some(-5.0,5.0),Nbins=1000,fn="dphi.txt")

df_hist(df1.withColumn("dphipull",F.degrees($"phi_s"-$"phi_t")*3600/$"sigpos"),"dphipull",Some(-15.0,15.0),Nbins=1001,fn="pullphi.txt")

//dtheta
df_hist(df1.withColumn("dtet",F.degrees($"theta_s"-$"theta_t")*3600),"dtet",Some(-5.0,5.0),Nbins=1001,fn="dtet.txt")

df_hist(df1.withColumn("dtet",F.degrees(F.cos($"theta_s")-F.cos($"theta_t"))*3600),"dtet",Some(-5.0,5.0),Nbins=1001,fn="dctet.txt")

df_hist(df1.withColumn("dtetpull",F.degrees($"theta_s"-$"theta_t")*3600/$"sigpos"),"dtetpull",Some(-15.0,15.0),Nbins=1000,fn="pulltet.txt")

//r
df_hist(df1,"r",Some(0,7),Nbins=1000,fn="r.txt")
df_hist(df1.withColumn("rnorm",$"r"/$"sigpos"),"rnorm",Some(0,25),Nbins=1000,fn="pullr.txt")
df_hist(df1,"cumr",Some(0,1),Nbins=1000,fn="cumr.txt")

//mags
df_hist(df1.withColumn("dmag",$"mag_i_cModel"-$"mag_i"),"dmag",Some(-15,10),Nbins=1000,fn="dmag.txt")
df_hist(df1.withColumn("pullmag",($"mag_i_cModel"-$"mag_i")/$"magerr_i_cModel"),"pullmag",Some(-300,150),Nbins=1000,fn="pullmag.txt")

//flux
df_hist(df1,"dflux",Some(-50000,50000),Nbins=1000,fn="dflux.txt")
df_hist(df1.withColumn("pullf",$"dflux"/$"cModelFluxErr_i"),"pullf",Some(-50,50),Nbins=1000,fn="pullflux.txt")
