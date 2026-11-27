import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Part C: does the existence wrapper block pushdown? Runs on the pushdown evidence's table of 1.2M
 * Patients sorted by birthDate in 412 row groups. Args: the pushdown work directory (with `full`), the data directory.
 */
object PartC {
  def main(args: Array[String]): Unit = {
    val spark = Common.session(pushdown = true)
    val ptc = PathlingContext.create(spark)
    val full = spark.read.parquet(args(0) + "/full")
    val total = full.count()
    println(s"rows $total; " + Common.footer(spark, args(0) + "/full", "_birthDate.__end"))
    val ts = lit(java.time.Instant.parse("2000-01-01T00:00:00Z"))
    val end = col("_birthDate").getField("__end")
    val wrapped = when(col("id").isNotNull, end)
    println("| Arm | PushedFilters | Rows scanned of " + total + " | Matches |")
    println("| --- | --- | --- | --- |")
    def run(label: String, df: DataFrame, show: Boolean = false): Unit = {
      val q = df.select("id")
      val matched = q.count()
      val scannedRows = Common.scanned(df.select("id"))
      println(f"| $label | ${Common.pushed(df.select("id")).take(160)} | $scannedRows%,d | $matched%,d |")
      if (show) println("   optimised filter: " + Common.optimisedFilter(df.select("id")))
    }
    val c1 = end.geq(ts)
    val c2 = wrapped.geq(ts)
    run("C1 `_birthDate.__end >= ts` (control)", full.filter(c1), show = true)
    run("C2 `CASE WHEN id IS NOT NULL THEN _birthDate.__end END >= ts`", full.filter(c2), show = true)
    run("C2' `CASE WHEN id IS NOT NULL THEN _birthDate.__end >= ts END`",
      full.filter(when(col("id").isNotNull, c1)), show = true)
    run("C3 C2 AND C1 (the D11 superset conjunct)", full.filter(c2.and(c1)), show = true)
    run("C3' C1 AND C2", full.filter(c1.and(c2)), show = true)
    val engine = ptc.fhirPathToColumn("Patient", "birthDate >= @2000")
    val engineDf = full.filter(engine)
    run("C4 engine `birthDate >= @2000`", engineDf, show = true)
    println("C4 analysed filter (before optimisation), shortened:")
    println(engineDf.queryExecution.analyzed.toString.take(1800))
    println("C4: contains isnotnull(id) after optimisation: " +
      Common.optimisedFilter(engineDf).contains("isnotnull(id"))
    println("C4: contains isnotnull(id) in the analysed plan: " +
      engineDf.queryExecution.analyzed.toString.contains("isnotnull(id"))
    // The same through a view, whose where clause may build the resource differently.
    val src = ptc.read().parquet(args(1) + "/pat_x")
    val view = src.view("Patient").json(
      """{"resourceType":"ViewDefinition","resource":"Patient","status":"active",
        |"where":[{"path":"birthDate >= @2000"}],
        |"select":[{"column":[{"path":"id","name":"id"}]}]}""".stripMargin).execute()
    println("view where `birthDate >= @2000`, optimised plan has isnotnull(id): " +
      view.queryExecution.optimizedPlan.toString.contains("isnotnull(id"))
    println("view analysed plan has isnotnull(id): " +
      view.queryExecution.analyzed.toString.contains("isnotnull(id"))
    println(view.queryExecution.optimizedPlan.toString.take(1200))
    spark.stop()
  }
}
