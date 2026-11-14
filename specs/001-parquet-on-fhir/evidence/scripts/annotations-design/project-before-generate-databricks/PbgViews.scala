package spike

import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.io.source.QueryableDataSource
import org.apache.spark.sql.{Dataset, PbgRewrite, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/**
 * Compute-first ("project before generate") on the engine views, rule off. For each view: the
 * original plan, and the plan rewritten so that a Project under the Generate computes the inline
 * input. The rule is never installed.
 */
object PbgViews {

  private def normalise(p: String): String =
    p.replaceAll("#\\d+L?", "#").replaceAll("\\d{6,}", "N")

  private def lines(plan: String, needle: String): Seq[String] =
    plan.linesIterator.filter(_.contains(needle)).map(_.trim.take(300)).toSeq

  private def one(spark: SparkSession, data: QueryableDataSource, label: String, v: String,
      full: StructType): Map[String, Any] = {
    def build(): Dataset[Row] = data.view("Patient").json(v).execute().asInstanceOf[Dataset[Row]]
    LambdaPruning.uninstall(spark)
    build().collect() // Warm-up, so that Delta's log is not counted in the measured bytes.
    PbgRewrite.rewrite(build()).collect()
    val c0 = PbgRewrite.counters
    val origDf = build()
    val orig = DbxProbe.execute(spark, origDf, full)
    val rwDf = PbgRewrite.rewrite(build())
    val rw = DbxProbe.execute(spark, rwDf, full)
    val rw2 = DbxProbe.execute(spark, PbgRewrite.rewrite(build()), full)
    val c1 = PbgRewrite.counters
    val minus1 = origDf.except(rwDf).count()
    val minus2 = rwDf.except(origDf).count()
    val gens = PbgRewrite.optimisedGenerates(rwDf)
    val optText = PbgRewrite.optimisedText(rwDf)
    val execText = rwDf.queryExecution.executedPlan.treeString
    val origExec = origDf.queryExecution.executedPlan.treeString
    val equal = orig.rows == rw.rows && orig.schema == rw.schema && minus1 == 0 && minus2 == 0
    println(s"[pbg] $label equal=$equal bytes ${orig.bytes} -> ${rw.bytes} " +
      s"reads ${orig.info("reads")} -> ${rw.info("reads")} generates=$gens")
    Map(
      "case" -> label,
      "original" -> Map("bytesRead" -> orig.bytes, "rowCount" -> orig.rows.size,
        "info" -> (orig.info - "executedPlan"),
        "pruneStructFieldsOptimised" -> lines(origDf.queryExecution.optimizedPlan.treeString, "prune_struct_fields"),
        "pruneStructFieldsExecuted" -> lines(origExec, "prune_struct_fields")),
      "rewritten" -> Map("bytesRead" -> rw.bytes, "rowCount" -> rw.rows.size,
        "info" -> (rw.info - "executedPlan"),
        "optimisedGenerates" -> gens,
        "pruneStructFieldsOptimised" -> lines(optText, "prune_struct_fields"),
        "pruneStructFieldsExecuted" -> lines(execText, "prune_struct_fields"),
        "optimisedPlan" -> normalise(optText).take(6000),
        "executedPlan" -> normalise(execText).take(6000)),
      "rewriteCounters(applied,nothingToDo,failed)" -> c0.zip(c1).map { case (a, b) => b - a },
      "resultsEqual" -> equal, "schemaEqual" -> (orig.schema == rw.schema),
      "exceptOrigMinusRw" -> minus1, "exceptRwMinusOrig" -> minus2,
      "planStable" -> (normalise(rw.optimized).replaceAll("x_\\d+", "x") ==
        normalise(rw2.optimized).replaceAll("x_\\d+", "x")),
      "readStable" -> (rw.info("reads") == rw2.info("reads")),
      "rewrittenRun2Equal" -> (rw2.rows == orig.rows))
  }

  private def runViews(spark: SparkSession, data: QueryableDataSource, views: Seq[(String, String)],
      kind: String): Seq[Map[String, Any]] = {
    val full = data.read("Patient").schema
    views.map { case (label, v) =>
      try one(spark, data, label, v, full)
      catch { case t: Throwable =>
        println(s"[pbg] $label FAILED $t")
        Map("case" -> label, "error" -> t.toString.take(800), "rewriteFailure" -> PbgRewrite.lastFailure)
      }
    }
  }

  /** Runs the views on `variant` (`parquet` or `delta-dv`) and writes `pbg-<variant>.json`. */
  def run(spark: SparkSession, ndjson: String, singular: String, base: String, out: String,
      variant: String): String = {
    IoStats.register(spark)
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    val ptc = PathlingContext.create(spark)
    val stored = s"$base/engine-$variant"
    val source = ptc.read().ndjson(ndjson)
    val facts = scala.collection.mutable.Map[String, Any]()
    val names: QueryableDataSource = variant match {
      case "parquet" =>
        source.write().saveMode("overwrite").parquet(stored)
        ptc.read().parquet(stored)
      case _ =>
        source.write().saveMode("overwrite").delta(stored)
        val t = s"delta.`$stored/Patient.parquet`"
        spark.sql(s"ALTER TABLE $t SET TBLPROPERTIES (delta.enableDeletionVectors = true)")
        spark.sql(s"DELETE FROM $t WHERE id IN ('p1', 'p5', 'p9')")
        val hist = spark.sql(s"DESCRIBE HISTORY $t").select("operation", "operationMetrics")
          .collect().map(_.toString).toSeq
        facts("deletionVectorsAdded") = hist.exists(h =>
          "numDeletionVectorsAdded -> (\\d+)".r.findFirstMatchIn(h).exists(_.group(1).toLong > 0))
        ptc.read().delta(stored)
    }
    val results = runViews(spark, names, EngineViews.NAME_VIEWS, variant) ++
      runViews(spark, ptc.read().parquet(singular), EngineViews.SINGULAR_VIEWS, "singular")
    val json = Js(Map("variant" -> variant, "facts" -> facts.toMap,
      "sparkVersion" -> spark.version,
      "photon" -> DbxProbe.conf(spark, "spark.databricks.photon.enabled"),
      "runtimeEngine" -> DbxProbe.conf(spark, "spark.databricks.clusterUsageTags.runtimeEngine"),
      "rewriteTotals(applied,nothingToDo,failed)" -> PbgRewrite.counters,
      "views" -> results))
    DbxProbe.writeText(spark, s"$out/pbg-$variant.json", json)
    json
  }
}
