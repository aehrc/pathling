import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession

/** Encodes the Observation NDJSON through the library API and prints the stored schema. */
object Schema {
  def main(args: Array[String]): Unit = {
    val data = args(0)
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ptc = PathlingContext.create(spark)
    ptc.read().ndjson(data + "/ndjson").write().saveMode("overwrite").parquet(data + "/encoded")
    new java.io.File(data + "/encoded").list().foreach(println)
    val df = spark.read.parquet(data + "/encoded/Observation.*.parquet")
    println(df.count())
    println(df.schema("valueQuantity").dataType.prettyJson)
    df.select("id", "valueQuantity").show(3, false)
    spark.stop()
  }
}
