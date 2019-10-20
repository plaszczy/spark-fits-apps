
:load df_tools.scala

//dphi
df_hist(df1.withColumn("dphi",F.degrees($"phi_s"-$"phi_t")*3600),"dphi",Some(-5.0,5.0),Nbins=1000,fn="dphi.txt")

df_hist(df1.withColumn("dtet",F.degrees($"theta_s"-$"theta_t")*3600),"dtet",Some(-5.0,5.0),Nbins=1000,fn="dtet.txt")

df_hist(df1.withColumn("dphipull",F.degrees($"phi_s"-$"phi_t")*3600/$"sigr"),"dphipull",Some(-15.0,15.0),Nbins=1000,fn="dphipull.txt")

df_hist(df1.withColumn("dtetpull",F.degrees($"theta_s"-$"theta_t")*3600/$"sigr"),"dtetpull",Some(-15.0,15.0),Nbins=1000,fn="dtetpull.txt")

df_hist(df1.withColumn("dmag",$"mag_i_cModel"-$"mag_i"),"dmag",fn="dmag.txt")

//cosmodc2 flux
val df2=df1.withColumn("flux_i",F.pow(F.lit(10),(F.lit(31.4)-$"mag_i")/2.5))
df_hist(df2.withColumn("dflux",$"cModelFlux_i"-$"flux_i"),"dflux",fn="dflux.txt")
