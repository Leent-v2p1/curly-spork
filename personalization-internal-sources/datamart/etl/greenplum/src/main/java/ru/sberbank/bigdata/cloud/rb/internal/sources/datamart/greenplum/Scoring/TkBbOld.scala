package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Scoring

import org.apache.spark.sql.SparkSession

object TkBbOld extends App {

  val spark: SparkSession = SparkSession.builder
    .appName("greenplum_stg_tk_bb")
    .enableHiveSupport()
    .getOrCreate()

  val user = new String(spark.conf.get("spark.jdbc.gpUser"))
  val destTable = new String(spark.conf.get("spark.jdbc.destTable"))
  val gpurl = new String(spark.conf.get("spark.jdbc.gpurl"))


  val v_stg_tk_bb_temp = spark.read.format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
    .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_hdp_vd")
    .option("dbtable", "v_stg_tk_bb_temp")
    .option("url", gpurl)
    .option("user", user)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", "epk_id")
    .load()

  v_stg_tk_bb_temp
    .repartition(10)
    .write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable(destTable)

}
