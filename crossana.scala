
:load df_tools.scala

//dphi
df_hist(df1.withColumn("dphi",F.degrees($"phi_s"-$"phi_t")*3600),"dphi",Some(-5.0,5.0),Nbins=1000,fn="dphi.txt")


df_hist(df1.withColumn("dtet",F.sin(($"theta_s"+$"theta_t")/2.0)*F.degrees($"theta_s"-$"theta_t")*3600),"dtet",Some(-5.0,5.0),Nbins=1000,fn="dtet.txt")
