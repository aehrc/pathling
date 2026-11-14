package spike

import au.csiro.pathling.library.PathlingContext
import au.csiro.pathling.library.io.source.QueryableDataSource
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.jdk.CollectionConverters._

/**
 * Case 5: the 18 views of EngineProbe, run through the real engine (library API) with the rule off
 * and on, on Parquet and on Delta with deletion vectors. The elements at the root of the resource
 * are top-level columns, which is what `name.family`, `birthDate` and `forEach: name` read.
 */
object EngineViews {

  private def column(path: String): String =
    "{\"column\":[{\"path\":\"" + path + "\",\"name\":\"r\",\"collection\":true}]"

  private def view(forEach: String, path: String): String = {
    val select = if (forEach == null) column(path) + "}" else column(path) + ",\"forEach\":\"" + forEach + "\"}"
    "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\"," +
      "\"select\":[" + select + "]}"
  }

  private val MULTI: String =
    "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\"," +
      "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},{\"path\":\"birthDate\",\"name\":\"bd\"}]}," +
      "{\"unionAll\":[{\"forEachOrNull\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]}," +
      "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}]," +
      "\"where\":[{\"path\":\"name.where(use = 'nickname').exists()\"}]}"

  /** The 14 views over the stored (extension-bearing) patients: label and view JSON. */
  val NAME_VIEWS: Seq[(String, String)] = Seq(
    (null, "name.family"), (null, "name.given"), (null, "name.period.start"),
    (null, "name.where(period.start > @1990).family"), (null, "name.where(use = 'nickname').given"),
    (null, "name.exists()"), (null, "name.count()"), (null, "address.period.start"),
    (null, "address.city | name.family"), ("name", "family"), ("name", "period.start"),
    (null, "name.first().family"), (null, "name.where(family.exists()).given"))
    .map { case (f, p) => ((if (f == null) "" else s"forEach $f / ") + s"`$p`", view(f, p)) } :+
    ("multi-column view with where and unionAll", MULTI)

  /** The 4 views over the singular-meta data. */
  val SINGULAR_VIEWS: Seq[(String, String)] = Seq(
    "birthDate", "birthDate.where($this > @1990)", "birthDate | deceasedDateTime",
    "(birthDate | deceasedDateTime).where($this > @1990).exists()")
    .map(p => (s"singular-meta small: `$p`", view(null, p)))

  private def runViews(spark: SparkSession, data: QueryableDataSource, views: Seq[(String, String)],
      kind: String): Seq[Map[String, Any]] = {
    val full = data.read("Patient").schema
    views.map { case (label, v) =>
      val r = DbxProbe.compare(spark, label, v, full,
        q => data.view("Patient").json(q).execute().asInstanceOf[Dataset[Row]])
      val off = r("off").asInstanceOf[Map[String, Any]]
      val on = r("on").asInstanceOf[Map[String, Any]]
      println(s"[engine $kind] $label equal=${r("resultsEqual")} bytes ${off("bytesRead")} -> " +
        s"${on("bytesRead")} counters(on)=${on("ruleCounters")}")
      r
    }
  }

  /** Runs the views on `variant` (`parquet` or `delta-dv`) and writes `engine-<variant>.json`. */
  def run(spark: SparkSession, ndjson: String, singular: String, base: String, out: String,
      variant: String): String = {
    IoStats.register(spark)
    LambdaPruning.reset()
    // The library refuses to store range annotations as INT96 timestamps.
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
        facts("history") = hist
        facts("deletionVectorsAdded") = hist.exists(h =>
          "numDeletionVectorsAdded -> (\\d+)".r.findFirstMatchIn(h).exists(_.group(1).toLong > 0))
        ptc.read().delta(stored)
    }
    val results = runViews(spark, names, NAME_VIEWS, variant) ++
      runViews(spark, ptc.read().parquet(singular), SINGULAR_VIEWS, "singular")
    val json = Js(Map(
      "variant" -> variant, "facts" -> facts.toMap,
      "relationClasses" -> LambdaPruning.relationClasses.asScala.map { case (k, v) => k -> v.get }.toMap,
      "unknownExpressions" -> LambdaPruning.unknownExprs.asScala.toMap,
      "bailReasons" -> LambdaPruning.bailReasons.asScala.toSeq,
      "views" -> results))
    DbxProbe.writeText(spark, s"$out/engine-$variant.json", json)
    json
  }
}
