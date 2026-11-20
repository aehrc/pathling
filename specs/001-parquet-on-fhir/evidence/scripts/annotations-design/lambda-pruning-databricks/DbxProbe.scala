package spike

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Try

/**
 * Sums the bytes that tasks read, per job group. The size of the files a scan lists does not
 * change with column pruning, so the bytes a task actually read are what shows a read was pruned.
 */
object IoStats extends SparkListener {
  // Slots: 0 bytes read, 1 jobs started, 2 jobs ended.
  private val groups = new ConcurrentHashMap[String, Array[AtomicLong]]
  private val stageGroup = new ConcurrentHashMap[Int, String]
  private val jobGroup = new ConcurrentHashMap[Int, String]
  @volatile private var registered = false

  private def slot(g: String): Array[AtomicLong] =
    groups.computeIfAbsent(g, _ => Array.fill(3)(new AtomicLong))

  def register(spark: SparkSession): Unit = synchronized {
    if (!registered) { spark.sparkContext.addSparkListener(this); registered = true }
  }

  override def onJobStart(e: SparkListenerJobStart): Unit = {
    val g = Option(e.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id"))).orNull
    if (g != null) {
      slot(g)(1).incrementAndGet()
      jobGroup.put(e.jobId, g)
      e.stageIds.foreach(stageGroup.put(_, g))
    }
  }

  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
    val g = stageGroup.get(e.stageId)
    if (g != null && e.taskMetrics != null)
      slot(g)(0).addAndGet(e.taskMetrics.inputMetrics.bytesRead)
  }

  override def onJobEnd(e: SparkListenerJobEnd): Unit = {
    val g = jobGroup.get(e.jobId)
    if (g != null) slot(g)(2).incrementAndGet()
  }

  /** Waits for every job of the group to end, then returns the bytes its tasks read. */
  def bytesRead(g: String): Long = {
    val s = slot(g)
    val deadline = System.currentTimeMillis() + 30000
    while ((s(1).get == 0 || s(2).get < s(1).get) && System.currentTimeMillis() < deadline)
      Thread.sleep(25)
    s(0).get
  }
}

/** Minimal JSON writer, so the probe does not depend on a JSON library on the cluster. */
object Js {
  def apply(a: Any): String = a match {
    case null => "null"
    case s: String => "\"" + esc(s) + "\""
    case b: Boolean => b.toString
    case n: java.lang.Number => n.toString
    case n: Int => n.toString
    case n: Long => n.toString
    case m: Map[_, _] =>
      m.map { case (k, v) => "\"" + esc(k.toString) + "\":" + apply(v) }.mkString("{", ",", "}")
    case s: Iterable[_] => s.map(apply).mkString("[", ",", "]")
    case s: Array[_] => s.map(apply).mkString("[", ",", "]")
    case o => apply(o.toString)
  }

  private def esc(s: String): String = {
    val b = new StringBuilder
    s.foreach {
      case '"' => b ++= "\\\""
      case '\\' => b ++= "\\\\"
      case '\n' => b ++= "\\n"
      case '\r' => b ++= "\\r"
      case '\t' => b ++= "\\t"
      case c if c < ' ' => b ++= f"\\u${c.toInt}%04x"
      case c => b += c
    }
    b.toString
  }
}

/**
 * Probes for the Databricks run: pruning is judged on the scan's read schema and on the bytes
 * its tasks read, with results compared between the rule off and on.
 */
object DbxProbe {

  private val groupSeq = new AtomicLong

  // ---- Plan inspection that works for any scan node, including Photon's. ----

  private def isScan(p: SparkPlan): Boolean =
    p.nodeName.contains("Scan") && !p.nodeName.contains("InMemory")

  /** Every scan node under a plan, unwrapping adaptive execution. */
  def scans(p: SparkPlan): Seq[SparkPlan] = {
    val kids: Seq[SparkPlan] = p match {
      case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
      case _ => p.children
    }
    (if (isScan(p)) Seq(p) else Nil) ++ kids.flatMap(scans)
  }

  /** The type in a `ReadSchema: struct<...>` fragment of a node's description. */
  private def parseReadSchema(desc: String): Option[StructType] = {
    val i = desc.indexOf("ReadSchema: struct<")
    if (i < 0) None
    else {
      val start = i + "ReadSchema: ".length
      var depth = 0
      var j = start
      var end = -1
      while (j < desc.length && end < 0) {
        desc.charAt(j) match {
          case '<' => depth += 1
          case '>' => depth -= 1; if (depth == 0) end = j
          case _ => ()
        }
        j += 1
      }
      if (end < 0) None
      else Try(DataType.fromDDL(desc.substring(start, end + 1)).asInstanceOf[StructType]).toOption
    }
  }

  /** The read schema of a scan node: its own, by reflection, or else parsed from its text. */
  private def readSchemaOf(p: SparkPlan): Option[StructType] = {
    val viaMethod = Try {
      p.getClass.getMethods.find(m => m.getName == "requiredSchema" && m.getParameterCount == 0)
        .map(_.invoke(p).asInstanceOf[StructType])
    }.toOption.flatten
    viaMethod.orElse(parseReadSchema(p.simpleString(1000000)))
  }

  /** A read schema as the leaves it reads, collapsing a fully read subtree to its root. */
  def compact(read: StructType, full: StructType): String = {
    def go(r: DataType, f: Option[DataType], path: String): Seq[String] = (r, f) match {
      case (_, Some(ff)) if r == ff => Seq(if (path.isEmpty) "*" else s"$path (whole)")
      case (rs: StructType, Some(fs: StructType)) => rs.fields.toSeq.flatMap { rf =>
        go(rf.dataType, fs.fields.find(_.name == rf.name).map(_.dataType),
          if (path.isEmpty) rf.name else s"$path.${rf.name}")
      }
      case (ArrayType(re, _), Some(ArrayType(fe, _))) => go(re, Some(fe), path + "[]")
      case (_, None) => Seq(s"$path (not in table schema)")
      case _ => Seq(path)
    }
    read.fields.toSeq.flatMap(rf =>
      go(rf.dataType, full.fields.find(_.name == rf.name).map(_.dataType), rf.name)).mkString(", ")
  }

  private def normalise(plan: String): String = plan.replaceAll("#\\d+L?", "#").replaceAll("\\d{6,}", "N")

  /** What the scans of an executed plan read, for any scan node class. */
  def describePlan(qe: org.apache.spark.sql.execution.QueryExecution, full: StructType)
      : Map[String, Any] = {
    val nodes = scans(qe.executedPlan)
    Map(
      "scanClasses" -> nodes.map(_.getClass.getSimpleName),
      "scanNames" -> nodes.map(_.nodeName.take(80)),
      "reads" -> nodes.map(n => readSchemaOf(n).map(compact(_, full))
        .getOrElse("UNPARSED: " + n.simpleString(400))),
      "readLeaves" -> nodes.map(n => readSchemaOf(n).map(s => leaves(s, "")).getOrElse(Nil)),
      "scanMetrics" -> nodes.map(n => n.metrics.map { case (k, m) => k -> m.value }.toMap),
      "executedPlan" -> qe.executedPlan.treeString.take(6000))
  }

  def leaves(t: DataType, prefix: String): Seq[String] = t match {
    case st: StructType => st.fields.toSeq.flatMap(f =>
      leaves(f.dataType, if (prefix.isEmpty) f.name else s"$prefix.${f.name}"))
    case ArrayType(et, _) => leaves(et, prefix + "[]")
    case _ => Seq(prefix)
  }

  // ---- Running one query. ----

  /** One execution: rows, schema, what the scans read, bytes read, and the rule's counters. */
  final case class Run(rows: Seq[String], schema: String, info: Map[String, Any], bytes: Long,
      counters: Seq[Long], optimized: String)

  def execute(spark: SparkSession, df: Dataset[Row], full: StructType): Run = {
    val g = s"lpr-${groupSeq.incrementAndGet()}"
    spark.sparkContext.setJobGroup(g, "lambda pruning probe", interruptOnCancel = false)
    val c0 = LambdaPruning.counters
    val rows = try df.collect().map(_.toString).sorted.toSeq
    finally spark.sparkContext.clearJobGroup()
    val bytes = IoStats.bytesRead(g)
    val c1 = LambdaPruning.counters
    val qe = df.queryExecution
    Run(rows, df.schema.simpleString, describePlan(qe, full), bytes,
      c0.zip(c1).map { case (a, b) => b - a }.toSeq, normalise(qe.optimizedPlan.treeString))
  }

  /** Whether the scans read the foreign annotation `__foreign` that the case data carries. */
  private def readsForeign(r: Run): Seq[String] =
    r.info("readLeaves").asInstanceOf[Seq[Seq[String]]].flatten.filter(_.contains("__foreign"))

  private def runMap(r: Run): Map[String, Any] = Map(
    "rowCount" -> r.rows.size,
    "foreignLeavesRead" -> readsForeign(r),
    "bytesRead" -> r.bytes,
    // Order: rewrites, no-ops, not applicable, bail-outs.
    "ruleCounters" -> r.counters,
    "info" -> r.info)

  /** A query with the rule off, then on (twice, and on a reused Dataset), compared. */
  def compare(spark: SparkSession, name: String, sql: String, full: StructType,
      df: String => Dataset[Row]): Map[String, Any] = {
    LambdaPruning.uninstall(spark)
    // Warm-up, so that Delta's log is cached and not counted in the bytes of a measured run.
    df(sql).collect()
    val off = execute(spark, df(sql), full)
    LambdaPruning.install(spark)
    val on1 = execute(spark, df(sql), full)
    val on2 = execute(spark, df(sql), full)
    val reused = df(sql)
    val r1 = reused.collect().map(_.toString).sorted.toSeq
    val r2 = reused.collect().map(_.toString).sorted.toSeq
    LambdaPruning.uninstall(spark)
    Map(
      "case" -> name,
      "off" -> runMap(off),
      "on" -> runMap(on1),
      "on2" -> runMap(on2),
      "resultsEqual" -> (off.rows == on1.rows && off.schema == on1.schema),
      "resultsEqualRun2" -> (off.rows == on2.rows),
      "resultsEqualReusedDataset" -> (r1 == off.rows && r2 == off.rows),
      "planStable" -> (on1.optimized == on2.optimized),
      "readStable" -> (on1.info("reads") == on2.info("reads")),
      "bytesStable" -> (math.abs(on1.bytes - on2.bytes) <= math.max(1024L, on1.bytes / 20)))
  }

  // ---- Data. ----

  /**
   * The case schema plus a foreign annotation, `__foreign`, that the engine does not recognise: in
   * `_birthDate` and in the period of each `location` element.
   */
  val SCHEMA: String = CaseProbe.SCHEMA
    .replace(s"_birthDate ${CaseProbe.META}",
      s"_birthDate ${CaseProbe.META.dropRight(1)},__foreign:string>")
    .replace(s"location array<struct<extension:${CaseProbe.EXT},period:${CaseProbe.PERIOD}>>",
      s"location array<struct<extension:${CaseProbe.EXT},period:" +
        s"${CaseProbe.PERIOD.dropRight(1)},__foreign:string>>>")

  /**
   * The rows of CaseProbe, with extension payloads made unique so that they do not compress, and
   * a unique `__foreign` value in every `_birthDate` and period. A period under `name` gets one
   * too, which the schema above drops.
   */
  def bigRows(n: Int): Seq[String] = {
    val rnd = new java.util.Random(42)
    val pat = "payload-(\\d)".r
    def hex(): String = java.lang.Long.toHexString(rnd.nextLong())
    CaseProbe.rows(n).map { s =>
      val unique = pat.replaceAllIn(s, m => s"payload-${hex()}-${m.group(1)}")
      "\"period\":\\{".r.replaceAllIn(
        "\"_birthDate\":\\{".r.replaceAllIn(unique, _ => s"\"_birthDate\":{\"__foreign\":\"fb${hex()}${hex()}\","),
        _ => s"\"period\":{\"__foreign\":\"fp${hex()}${hex()}\",")
    }
  }

  /** Writes the case data as each kind of table under `base` and returns the table paths. */
  def prepare(spark: SparkSession, base: String, kinds: Seq[String], nrows: Int)
      : Map[String, Map[String, Any]] = {
    import spark.implicits._
    val src = s"$base/source.parquet"
    if (!exists(spark, src)) {
      spark.read.schema(SCHEMA).json(bigRows(nrows).toDS()).repartition(4)
        .write.mode("overwrite").parquet(src)
    }
    val data = spark.read.parquet(src)
    def reset(): Unit = Seq("enableDeletionVectors", "columnMapping.mode").foreach(k =>
      spark.conf.unset(s"spark.databricks.delta.properties.defaults.$k"))
    def put(k: String, v: String): Unit =
      spark.conf.set(s"spark.databricks.delta.properties.defaults.$k", v)
    def writeDelta(path: String, props: Map[String, String], cluster: Boolean): Unit = {
      reset(); props.foreach { case (k, v) => put(k, v) }
      val w = data.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
      (if (cluster) w.clusterBy("id") else w).save(path)
      reset()
    }
    def deleteSome(path: String): Unit =
      spark.sql(s"DELETE FROM delta.`$path` WHERE id LIKE 'p1%' OR id LIKE 'p77%'")
    kinds.map { kind =>
      val path = s"$base/$kind"
      kind match {
        case "parquet" => data.write.mode("overwrite").parquet(path)
        case "delta-dv" =>
          writeDelta(path, Map("enableDeletionVectors" -> "true"), cluster = false); deleteSome(path)
        case "delta-colmap" =>
          writeDelta(path, Map("columnMapping.mode" -> "name", "enableDeletionVectors" -> "false"),
            cluster = false)
        case "delta-liquid" =>
          writeDelta(path, Map("enableDeletionVectors" -> "false"), cluster = true)
          spark.sql(s"OPTIMIZE delta.`$path`")
        case "delta-all" =>
          writeDelta(path, Map("columnMapping.mode" -> "name", "enableDeletionVectors" -> "true"),
            cluster = true)
          spark.sql(s"OPTIMIZE delta.`$path`")
          deleteSome(path)
      }
      val facts: Map[String, Any] =
        if (kind == "parquet") Map("path" -> path)
        else {
          val detail = spark.sql(s"DESCRIBE DETAIL delta.`$path`").collect().head
          val hist = spark.sql(s"DESCRIBE HISTORY delta.`$path`").select("operation", "operationMetrics")
            .collect().map(_.toString).toSeq
          Map("path" -> path, "properties" -> detail.getAs[Any]("properties").toString,
            "clusteringColumns" -> Option(detail.getAs[Any]("clusteringColumns")).map(_.toString).orNull,
            "numFiles" -> detail.getAs[Long]("numFiles"),
            "history" -> hist,
            "deletionVectorsAdded" -> hist.exists(h =>
              "numDeletionVectorsAdded -> (\\d+)".r.findFirstMatchIn(h).exists(_.group(1).toLong > 0)))
        }
      kind -> facts
    }.toMap
  }

  def loader(spark: SparkSession, kind: String, path: String): Unit = {
    val d = if (kind == "parquet") spark.read.parquet(path) else spark.read.format("delta").load(path)
    d.createOrReplaceTempView("t")
  }

  private def fs(spark: SparkSession, p: String) = {
    val path = new org.apache.hadoop.fs.Path(p)
    (path.getFileSystem(spark.sparkContext.hadoopConfiguration), path)
  }

  def exists(spark: SparkSession, p: String): Boolean = { val (f, q) = fs(spark, p); f.exists(q) }

  /** Writes text through the Hadoop file system, so that any path scheme the cluster has works. */
  def writeText(spark: SparkSession, p: String, text: String): Unit = {
    val (f, q) = fs(spark, p)
    val o = f.create(q, true)
    try o.write(text.getBytes(StandardCharsets.UTF_8)) finally o.close()
  }

  /** A session conf as text, or `?` where it is unset or not a typed conf of this runtime. */
  def conf(spark: SparkSession, key: String): String =
    Try(spark.conf.get(key)).getOrElse("?")

  /** The runtime's version and the shapes of the internal classes the rule depends on. */
  def runtimeFacts(spark: SparkSession): Map[String, Any] = {
    import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
    def ctors(c: Class[_]): Seq[String] = c.getDeclaredConstructors.map(_.toGenericString).toSeq
    def methods(c: Class[_], n: String): Seq[String] =
      c.getMethods.filter(_.getName == n).map(_.toGenericString).toSeq
    Map(
      "dbr" -> conf(spark, "spark.databricks.clusterUsageTags.sparkVersion"),
      "sparkVersion" -> spark.version,
      "scala" -> scala.util.Properties.versionString,
      "java" -> System.getProperty("java.version"),
      "logicalRelationConstructors" -> ctors(classOf[LogicalRelation]),
      "hadoopFsRelationConstructors" -> ctors(classOf[HadoopFsRelation]),
      "hadoopFsRelationCopy" -> methods(classOf[HadoopFsRelation], "copy"),
      "logicalRelationSource" ->
        Try(classOf[LogicalRelation].getProtectionDomain.getCodeSource.getLocation.toString).getOrElse("?"))
  }

  private def isControl(name: String): Boolean = Seq("1e", "1f", "1j", "4d").exists(name.startsWith)

  /** Cases 1 to 4 on each table kind. Writes `results-<kind>.json` to `out` and returns it. */
  def matrix(spark: SparkSession, base: String, out: String, kinds: Seq[String], nrows: Int,
      only: Seq[String]): String = {
    IoStats.register(spark)
    val tables = prepare(spark, base, kinds, nrows)
    val cases = if (only.isEmpty) CaseProbe.CASES
      else CaseProbe.CASES.filter { case (n, _) => only.exists(n.startsWith) }
    val results = kinds.map { kind =>
      val path = tables(kind)("path").toString
      loader(spark, kind, path)
      val full = spark.table("t").schema
      // Case 1j concatenates `_birthDate` with a `name[]._family`, which only analyses when the two
      // have the same type. The foreign annotation makes them differ, so it reads a view without it.
      spark.sql("select id, birthDate, named_struct('id', _birthDate.id, 'extension', " +
        "_birthDate.extension, '__start', _birthDate.__start, '__end', _birthDate.__end) as " +
        "_birthDate, name, location, dosage from t").createOrReplaceTempView("t_nf")
      val runs = cases.map { case (name, sql0) =>
        val sql = if (name.startsWith("1j")) sql0.replace(" from t", " from t_nf") else sql0
        val r = try compare(spark, name, sql, full, q => spark.sql(q))
        catch {
          case e: Exception =>
            println(s"[$kind] ${name.take(60)} ERROR ${e.toString.take(300)}")
            Map[String, Any]("case" -> name, "error" -> e.toString.take(1500),
              "resultsEqual" -> false)
        }
        if (r.contains("error")) r + ("control" -> isControl(name)) else {
        println(s"[$kind] ${name.take(60)} equal=${r("resultsEqual")} " +
          s"bytes ${r("off").asInstanceOf[Map[String, Any]]("bytesRead")} -> " +
          s"${r("on").asInstanceOf[Map[String, Any]]("bytesRead")} " +
          s"counters(on)=${r("on").asInstanceOf[Map[String, Any]]("ruleCounters")}")
        r + ("control" -> isControl(name))
        }
      }
      Map("kind" -> kind, "table" -> tables(kind), "fullSchema" -> full.simpleString.take(3000),
        "cases" -> runs)
    }
    val json = Js(Map(
      "sparkVersion" -> spark.version,
      "runtime" -> runtimeFacts(spark),
      "conf" -> Map(
        "adaptive" -> conf(spark, "spark.sql.adaptive.enabled"),
        "diskCache" -> conf(spark, "spark.databricks.io.cache.enabled"),
        "photon" -> conf(spark, "spark.databricks.photon.enabled"),
        "vectorized" -> conf(spark, "spark.sql.parquet.enableVectorizedReader")),
      "relationClasses" -> {
        import scala.jdk.CollectionConverters._
        LambdaPruning.relationClasses.asScala.map { case (k, v) => k -> v.get }.toMap
      },
      "unknownExpressions" -> {
        import scala.jdk.CollectionConverters._
        LambdaPruning.unknownExprs.asScala.toMap
      },
      "bailReasons" -> {
        import scala.jdk.CollectionConverters._
        LambdaPruning.bailReasons.asScala.toSeq
      },
      "results" -> results))
    writeText(spark, s"$out/matrix-${kinds.mkString("+")}.json", json)
    json
  }

  // ---- Entry points that take only plain types, for calls from Python through py4j. ----

  def caseSql(prefix: String): String = CaseProbe.CASES.find(_._1.startsWith(prefix)).get._2

  def prepareOne(spark: SparkSession, base: String, kind: String, nrows: Int): String = {
    IoStats.register(spark)
    prepare(spark, base, Seq(kind), nrows)(kind)("path").toString
  }

  def runMatrix(spark: SparkSession, base: String, out: String, kindsCsv: String, nrows: Int,
      onlyCsv: String): String =
    matrix(spark, base, out, kindsCsv.split(",").toSeq, nrows,
      if (onlyCsv.isEmpty) Nil else onlyCsv.split(",").toSeq)

  /** Describes a Python-built Dataset, to check the rule through py4j. */
  def describeDataset(df: Dataset[Row], fullDdl: String): String = {
    val full = DataType.fromDDL(fullDdl).asInstanceOf[StructType]
    df.collect()
    Js(describePlan(df.queryExecution, full) + ("counters" -> LambdaPruning.counters.toSeq))
  }

  /** Entry point for a JAR task, which installs the rule from Scala: `<base> <out> <nrows>`. */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    LambdaPruning.reset()
    LambdaPruning.install(spark)
    val ok = LambdaPruning.counters
    println(s"installed from Scala; counters=${ok.toSeq}")
    val json = matrix(spark, args(0), args(1), Seq("parquet", "delta-dv"), args(2).toInt,
      Seq("1b", "2c", "4b"))
    println(s"SCALA-SMOKE-DONE bytes=${json.length}")
  }
}
