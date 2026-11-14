package spike

import org.apache.spark.sql.SparkSession

/**
 * Where the rule can be injected. Usage: InjectionProbe <data dir> <mode>, one mode per JVM:
 *  - extra: a plain session, then spark.experimental.extraOptimizations (runtime);
 *  - operator: a new session built with injectOptimizerRule (operator optimisation batches);
 *  - precbo: a new session built with injectPreCBORule (the batch just before SchemaPruning);
 *  - builder-existing: a plain session, then builder().withExtensions(injectOptimizerRule)
 *    .getOrCreate(), to see whether extensions reach a session that already exists.
 * Each mode runs a few cases with the rule disabled and enabled through a session conf.
 * Expects CaseProbe to have written cases.parquet.
 */
object InjectionProbe {
  def main(args: Array[String]): Unit = {
    val mode = args(1)
    def base = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
    val spark = mode match {
      case "extra" =>
        val s = base.getOrCreate(); LambdaPruning.install(s); s
      case "operator" => base.withExtensions(_.injectOptimizerRule(_ => LambdaPruning)).getOrCreate()
      case "precbo" => base.withExtensions(_.injectPreCBORule(_ => LambdaPruning)).getOrCreate()
      case "delta" | "delta-colmap" =>
        val s = base
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
        LambdaPruning.install(s); s
      case "builder-existing" =>
        base.getOrCreate()
        base.withExtensions(_.injectOptimizerRule(_ => LambdaPruning)).getOrCreate()
    }
    spark.sparkContext.setLogLevel("ERROR")
    val dir = args(0) + "/cases.parquet"
    val full = spark.read.parquet(dir).schema
    spark.read.parquet(dir).createOrReplaceTempView("t")
    if (mode.startsWith("delta")) {
      // The same data as a Delta table, optionally with column mapping (physical names differ).
      val d = args(0) + "/cases." + mode
      val w = spark.read.parquet(dir).write.format("delta").mode("overwrite")
      (if (mode == "delta-colmap") w.option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2").option("delta.minWriterVersion", "5") else w).save(d)
      spark.read.format("delta").load(d).createOrReplaceTempView("t")
    }
    val picks = CaseProbe.CASES.filter { case (n, _) => Seq("1b", "2c", "3b", "4b").exists(n.startsWith) }
    for ((name, sql) <- picks) {
      spark.conf.set("spike.lambdaPruning.enabled", "false")
      val off = spark.sql(sql)
      val offRows = off.collect().map(_.toString).sorted.toSeq
      spark.conf.set("spike.lambdaPruning.enabled", "true")
      val on = spark.sql(sql)
      val onRows = on.collect().map(_.toString).sorted.toSeq
      println(s"| $mode | ${name.take(2)} | ${CaseProbe.compact(CaseProbe.readSchema(on), full)} | " +
        s"${if (offRows == onRows) "yes" else "**NO**"} |")
    }
    spark.stop()
  }
}
