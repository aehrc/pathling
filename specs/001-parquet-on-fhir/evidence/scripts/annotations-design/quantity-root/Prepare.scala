import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Builds the datasets of the probes under the data directory (args(0)); the Patient source is the
 * pushdown evidence's sorted table (args(1)).
 *
 *  - encoded: Observations encoded through the library API (NDJSON in args(0)/ndjson).
 *  - obs_engine: valueQuantity of today's layout (`__canonical_exact` struct), plus `__foreign`.
 *  - obs_flat: the proposed flat fields `__canonical_code`, `__canonical_value_exact`, plus
 *    `__foreign` and, for the pushdown comparison, the optional fixed-scale `__canonical_value`.
 *  - obs_no_vq: Observations without a valueQuantity column.
 *  - pat_*: Patients sampled from the pushdown table: with `_birthDate`, without it, and with the
 *    flat parent-level `__birthDate_start` and `__birthDate_end`.
 */
object Prepare {
  def main(args: Array[String]): Unit = {
    val data = args(0)
    val spark = Common.session(blockSize = Some("262144"))
    val ptc = PathlingContext.create(spark)
    if (!new java.io.File(data + "/encoded").exists()) {
      ptc.read().ndjson(data + "/ndjson").write().saveMode("overwrite").parquet(data + "/encoded")
    }
    val enc = spark.read.parquet(data + "/encoded/Observation.*.parquet")
    val vq = col("valueQuantity")
    val names = enc.schema("valueQuantity").dataType.asInstanceOf[StructType].fieldNames.toSeq
    println("valueQuantity fields today: " + names.mkString(", "))
    // About 200 characters of a foreign annotation, different in every row so that it cannot be
    // dictionary-encoded away.
    val foreign = concat(sha2(col("id"), 512), sha2(col("id"), 256),
      substring(sha2(col("id"), 224), 1, 8)).as("__foreign")
    val own = names.map(n => vq.getField(n).as(n))

    def write(name: String, quantity: org.apache.spark.sql.Column): Unit = {
      val df = enc.withColumn("valueQuantity", quantity)
        .sort(col("valueQuantity._value.__numeric")).coalesce(1)
      df.write.mode("overwrite").parquet(data + "/" + name + "/Observation.parquet")
      println(name + " " + Common.footer(spark, data + "/" + name + "/Observation.parquet",
        "valueQuantity.__canonical_exact.value.value"))
    }
    write("obs_engine", struct(own :+ foreign: _*))

    val exact = vq.getField("__canonical_exact")
    val scale = exact.getField("value").getField("scale")
    // Unscaled value over ten to the scale, at a fixed scale of 12.
    val fixed = (exact.getField("value").getField("value").cast("decimal(38,12)") /
      expr("cast(power(10, valueQuantity.__canonical_exact.value.scale) as decimal(38,0))")
        .cast("decimal(38,12)")).cast("decimal(38,12)")
    val flatOwn = names.filter(_ != "__canonical_exact").map(n => vq.getField(n).as(n))
    write("obs_flat", struct(flatOwn ++ Seq(
      exact.getField("code").as("__canonical_code"),
      exact.getField("value").as("__canonical_value_exact"),
      fixed.as("__canonical_value"), foreign): _*))
    enc.drop("valueQuantity").coalesce(1).write.mode("overwrite")
      .parquet(data + "/obs_no_vq/Observation.parquet")
    println("scale distribution: ")
    spark.read.parquet(data + "/obs_engine/Observation.parquet")
      .groupBy(col("valueQuantity.__canonical_exact.value.scale")).count().show()

    // Patients: a sample, ordered as the source is, and its variants.
    val full = spark.read.parquet(args(1) + "/full")
    full.printSchema()
    val s = full.where(pmod(hash(col("id")), lit(12)) === 0)
    val start = col("_birthDate.__start").as("__birthDate_start")
    val end = col("_birthDate.__end").as("__birthDate_end")
    val rest = full.columns.filter(_ != "_birthDate").map(col)
    def w(name: String, df: org.apache.spark.sql.DataFrame): Unit = {
      df.coalesce(1).write.mode("overwrite").parquet(data + "/" + name + "/Patient.parquet")
      println(name + " " + spark.read.parquet(data + "/" + name + "/Patient.parquet").count())
    }
    w("pat_x", s)
    w("pat_none", s.select(rest: _*))
    w("pat_flat_both", s.select(rest :+ start :+ end: _*))
    w("pat_flat_start", s.select(rest :+ start: _*))
    spark.stop()
  }
}
