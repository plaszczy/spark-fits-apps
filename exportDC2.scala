


val df_t=spark.read.parquet(System.getenv("COSMODC2")).coalesce(200)

val df=df_t.filter($"mag_i"<25.3).filter(df_t("ra").between(60,61)).filter(df_t("dec").between(-36,-35))

df.write.parquet("cosmoDC2_gold_1deg.parquet")



val df_t=spark.read.parquet(System.getenv("RUN2")).coalesce(200)

val df=df_t.filter($"mag_i"<25.3).filter(df_t("ra").between(60,61)).filter(df_t("dec").between(-36,-35))

df.write.parquet("run2_gold_1deg.parquet")



