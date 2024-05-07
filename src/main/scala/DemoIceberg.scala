import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object DemoIceberg {

  private val logger = LogManager.getLogger(DemoIceberg.getClass);

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("iceberg.auto.create.all", "true")
      .config("spark.sql.catalog.spark_catalog.warehouse", "/Users/srirajkadimisetty/projects/demos/e2e-prototype/approach-1/staging-area/warehouse")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("spark.local.dir", "/Users/srirajkadimisetty/projects/demos/e2e-prototype/approach-1/staging-area/warehouse/temp")
      .master("local[*]").getOrCreate();

    val data = Seq(
      (1L, "First record", "No error", true),
      (2L, "Second record", "No error", true)
    )

    val df: DataFrame = spark.createDataFrame(data).toDF("recordNumber", "record", "output_error", "apiInvocation")
    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS demo.demo_table (
        |  recordNumber LONG,
        |  record STRING,
        |  output_error STRING,
        |  apiInvocation BOOLEAN
        |)
        |USING iceberg
  """.stripMargin)

    df.writeTo("demo.`demo_table`").overwritePartitions()
    println("Table created and data written successfully.")
    spark.stop()
  }

}
