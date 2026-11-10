import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.LayerRewrites;
import org.apache.spark.sql.PlanRewrite;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import spike.CaseProbe;
import spike.LambdaPruning;

/**
 * Builds views through the real engine and compares five plans of each, all by rewriting the
 * analysed plan (the engine is not changed): O original; P compute-first only; R existence and
 * element-shape rewrites only; PR both; and, with the lambda-pruning rule ("D3b") installed,
 * PRD (PR) and D (the original). Prints one tab-separated ROW line per view and DIFF lines for
 * any row that differs from O. Usage: ProbeMain dataDir ndjsonDir singularParquetDir.
 */
public class ProbeMain {

  static String view(final String select, final String where) {
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
        + "\"select\":[" + select + "]"
        + (where == null ? "" : ",\"where\":[{\"path\":\"" + where + "\"}]") + "}";
  }

  static String col(final String path, final String name) {
    return "{\"path\":\"" + path + "\",\"name\":\"" + name + "\",\"collection\":true}";
  }

  static String simple(final String forEach, final String path) {
    final String column = "{\"column\":[" + col(path, "r") + "]";
    return view(forEach == null ? column + "}" : column + ",\"forEach\":\"" + forEach + "\"}", null);
  }

  static final String FAM = "\"column\":[{\"path\":\"family\",\"name\":\"f\"}]";

  /** One data set, its full schema and the views to run on it. */
  static class Source {
    final String name;
    final QueryableDataSource data;
    final StructType full;
    final List<String[]> views = new ArrayList<>();

    Source(final String name, final QueryableDataSource data, final StructType full) {
      this.name = name;
      this.data = data;
      this.full = full;
    }
  }

  static List<String[]> nameViews() {
    final List<String[]> v = new ArrayList<>();
    final String[][] nameCases = {
      {null, "name.family"},
      {null, "name.given"},
      {null, "name.period.start"},
      {null, "name.where(period.start > @1990).family"},
      {null, "name.where(use = 'nickname').given"},
      {null, "name.exists()"},
      {null, "name.count()"},
      {null, "address.period.start"},
      {null, "address.city | name.family"},
      {"name", "family"},
      {"name", "period.start"},
      {null, "name.first().family"},
      {null, "name.where(family.exists()).given"},
    };
    for (final String[] c : nameCases) {
      v.add(new String[] {
        (c[0] == null ? "" : "forEach " + c[0] + " / ") + "`" + c[1] + "`", simple(c[0], c[1]), "base"});
    }
    v.add(new String[] {"multi-column view with where and unionAll",
        "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
            + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},"
            + "{\"path\":\"birthDate\",\"name\":\"bd\"}]},"
            + "{\"unionAll\":[{\"forEachOrNull\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]},"
            + "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}],"
            + "\"where\":[{\"path\":\"name.where(use = 'nickname').exists()\"}]}", "base"});
    v.add(new String[] {"nested forEach name / forEach given",
        view("{\"forEach\":\"name\",\"select\":[{" + FAM + "},{\"forEach\":\"given\","
            + "\"column\":[{\"path\":\"$this\",\"name\":\"g\"}]}]}", null), "base"});
    v.add(new String[] {"forEachOrNull name / family",
        view("{\"forEachOrNull\":\"name\"," + FAM + "}", null), "base"});
    v.add(new String[] {"forEach name / family, where on annotation-free leaf (use)",
        view("{\"forEach\":\"name\"," + FAM + "}", "name.where(use = 'nickname').exists()"), "base"});
    v.add(new String[] {"forEach name / family, where with exists(criteria)",
        view("{\"forEach\":\"name\"," + FAM + "}", "name.exists(use = 'nickname')"), "base"});
    v.add(new String[] {"forEach name / family, where first().family = 'F1' (annotation-free leaf)",
        view("{\"forEach\":\"name\"," + FAM + "}", "name.first().family = 'F1'"), "base"});
    v.add(new String[] {"unionAll of two forEach (name family, address city)",
        view("{\"unionAll\":[{\"forEach\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]},"
            + "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}", null), "base"});
    v.add(new String[] {"forEach name / family, where on period.start (annotated leaf)",
        view("{\"forEach\":\"name\"," + FAM + "}", "name.first().period.start > @1990"), "base"});
    v.add(new String[] {"forEach name / period.start compared in column",
        view("{\"forEach\":\"name\",\"column\":[{\"path\":\"period.start > @1990\",\"name\":\"c\"}]}",
            null), "base"});
    // Views beyond the 26 of the previous spike, to exercise the existence rewrites.
    final String[][] extra = {
      {null, "name.where(use = 'nickname').exists()"},
      {null, "name.exists(period.start > @1990)"},
      {null, "name.all(use = 'nickname')"},
      {null, "name.empty()"},
      {null, "name.where(use = 'nickname').count()"},
      {null, "name.where(use = 'nickname').empty()"},
      {null, "name.given.count()"},
      {null, "address.exists(city = 'C1')"},
      {null, "name.all(period.start > @1990)"},
    };
    for (final String[] c : extra) {
      v.add(new String[] {"extra: `" + c[1] + "`", simple(c[0], c[1]), "extra"});
    }
    v.add(new String[] {"extra: forEach name / family, where all(given.exists())",
        view("{\"forEach\":\"name\"," + FAM + "}", "name.all(given.exists())"), "extra"});
    return v;
  }

  static List<String[]> singularViews() {
    final List<String[]> v = new ArrayList<>();
    final String[] cases = {
      "birthDate",
      "birthDate.where($this > @1990)",
      "birthDate | deceasedDateTime",
      "(birthDate | deceasedDateTime).where($this > @1990).exists()",
    };
    for (final String p : cases) {
      v.add(new String[] {"singular-meta small: `" + p + "`", simple(null, p), "base"});
    }
    return v;
  }

  static List<String> rows(final Dataset<Row> df) {
    return df.collectAsList().stream().map(Row::toString).sorted().toList();
  }

  static String reads(final Dataset<Row> df, final StructType full) {
    return CaseProbe.compact(CaseProbe.readSchema(df), full).replace(" (whole)", "");
  }

  /** A, B or C of an arm's read leaves, against the rule-on reference and the original read. */
  static String cls(final Set<String> arm, final Set<String> rule, final Set<String> original) {
    if (rule.containsAll(arm)) {
      return "A";
    }
    return arm.equals(original) ? "C" : "B";
  }

  static String diff(final String tag, final Dataset<Row> a, final Dataset<Row> b,
      final List<String> aRows, final List<String> bRows, final String label) {
    final long m1 = a.except(b).count();
    final long m2 = b.except(a).count();
    final boolean eq = aRows.equals(bRows) && m1 == 0 && m2 == 0
        && a.schema().catalogString().equals(b.schema().catalogString());
    if (!eq) {
      final List<String> onlyA = new ArrayList<>(aRows);
      onlyA.removeAll(bRows);
      final List<String> onlyB = new ArrayList<>(bRows);
      onlyB.removeAll(aRows);
      System.out.println("DIFF\t" + label + "\t" + tag + "\texceptAB=" + m1 + " exceptBA=" + m2
          + "\tonlyInO=" + onlyA.subList(0, Math.min(5, onlyA.size())) + "\tonlyInArm="
          + onlyB.subList(0, Math.min(5, onlyB.size())));
    }
    return eq ? "yes" : "NO";
  }

  static void runView(final SparkSession spark, final Source src, final String[] view) {
    final String label = src.name + view[0];
    try {
      LambdaPruning.uninstall(spark);
      final Dataset<Row> o = src.data.view("Patient").json(view[1]).execute();
      final Set<String> lo = LayerRewrites.leafSet(o);
      final String ro = reads(o, src.full);
      final List<String> oRows = rows(o);
      final Dataset<Row> p = PlanRewrite.rewrite(src.data.view("Patient").json(view[1]).execute());
      final Set<String> lp = LayerRewrites.leafSet(p);
      final List<String> pRows = rows(p);
      LayerRewrites.reset();
      final Dataset<Row> r =
          LayerRewrites.run(src.data.view("Patient").json(view[1]).execute(), true, true, false);
      final String applied = LayerRewrites.summary();
      final Set<String> lr = LayerRewrites.leafSet(r);
      final List<String> rRows = rows(r);
      final Dataset<Row> pr =
          LayerRewrites.run(src.data.view("Patient").json(view[1]).execute(), true, true, true);
      final Set<String> lpr = LayerRewrites.leafSet(pr);
      final List<String> prRows = rows(pr);
      final String rp = reads(p, src.full);
      final String rr = reads(r, src.full);
      final String rpr = reads(pr, src.full);
      LambdaPruning.install(spark);
      final Dataset<Row> d = src.data.view("Patient").json(view[1]).execute();
      final Set<String> ld = LayerRewrites.leafSet(d);
      final String rd = reads(d, src.full);
      final List<String> dRows = rows(d);
      final Dataset<Row> prd =
          LayerRewrites.run(src.data.view("Patient").json(view[1]).execute(), true, true, true);
      final Set<String> lprd = LayerRewrites.leafSet(prd);
      final String rprd = reads(prd, src.full);
      final List<String> prdRows = rows(prd);
      // The except comparisons run with the rule off, so that they read like the originals.
      LambdaPruning.uninstall(spark);
      final String eqP = diff("P", o, p, oRows, pRows, label);
      final String eqR = diff("R", o, r, oRows, rRows, label);
      final String eqPR = diff("PR", o, pr, oRows, prRows, label);
      final String eqD = diff("D", o, d, oRows, dRows, label);
      final String eqPRD = diff("PRD", o, prd, oRows, prdRows, label);
      final boolean strict = o.schema().equals(pr.schema());
      final boolean composes = lpr.containsAll(lprd) && ld.containsAll(lprd);
      System.out.println("ROW\t" + String.join("\t", label.replace("|", "\\|"), ro, rp, rr, rpr, rprd, rd,
          eqP, eqR, eqPR, eqPRD, eqD, String.valueOf(oRows.size()), applied,
          cls(lp, ld, lo), cls(lr, ld, lo), cls(lpr, ld, lo), cls(lprd, ld, lo),
          String.valueOf(composes), String.valueOf(strict), view[2]));
    } catch (final Exception e) {
      LambdaPruning.uninstall(spark);
      System.out.println("ERROR\t" + label + "\t" + String.valueOf(e).replace("\n", " "));
      e.printStackTrace(System.out);
    }
  }

  /** Writes a Parquet on FHIR Patient table whose name and address hold null elements. */
  static void writeEdge(final SparkSession spark, final String stored, final String out) {
    final Dataset<Row> base = spark.read().parquet(stored + "/Patient.*.parquet");
    final StructType schema = base.schema();
    final String nameT = ((org.apache.spark.sql.types.ArrayType) schema.apply("name").dataType())
        .elementType().sql();
    final String addrT = ((org.apache.spark.sql.types.ArrayType) schema.apply("address").dataType())
        .elementType().sql();
    final String nn = "cast(null as " + nameT + ")";
    final String an = "cast(null as " + addrT + ")";
    final String[] cols = schema.fieldNames();
    final List<Dataset<Row>> parts = new ArrayList<>();
    parts.add(base);
    // Null elements around and between the real ones; a null element alone.
    final String[] variants = {
      "concat(array(" + nn + "), coalesce(name, cast(array() as array<" + nameT + ">)), array(" + nn + "))",
      "array(" + nn + ")",
    };
    final String[] avariants = {
      "concat(array(" + an + "), coalesce(address, cast(array() as array<" + addrT + ">)), array(" + an + "))",
      "array(" + an + ")",
    };
    for (int i = 0; i < variants.length; i++) {
      final List<String> exprs = new ArrayList<>();
      for (final String c : cols) {
        if (c.equals("name")) {
          exprs.add(variants[i] + " as name");
        } else if (c.equals("address")) {
          exprs.add(avariants[i] + " as address");
        } else if (c.equals("id")) {
          exprs.add("concat(id, '-nullelem" + i + "') as id");
        } else {
          exprs.add(c);
        }
      }
      parts.add(base.selectExpr(exprs.toArray(new String[0])));
    }
    Dataset<Row> all = parts.get(0);
    for (int i = 1; i < parts.size(); i++) {
      all = all.unionByName(parts.get(i));
    }
    all.coalesce(1).write().mode("overwrite").parquet(out + "/Patient.parquet");
  }

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    LambdaPruning.verbose_$eq(false);
    final PathlingContext ptc = PathlingContext.create(spark);
    final String stored = args[0] + "/engine";
    ptc.read().ndjson(args[1]).write().saveMode("overwrite").parquet(stored);
    System.out.println("Spark " + spark.version());
    final Source names = new Source("", ptc.read().parquet(stored),
        spark.read().parquet(stored + "/Patient.*.parquet").schema());
    names.views.addAll(nameViews());
    final String edgeDir = args[0] + "/edge";
    writeEdge(spark, stored, edgeDir);
    final Source edge = new Source("edge: ", ptc.read().parquet(edgeDir),
        spark.read().parquet(edgeDir + "/Patient.parquet").schema());
    edge.views.addAll(nameViews());
    final Source singular = new Source("", ptc.read().parquet(args[2]),
        spark.read().parquet(args[2] + "/Patient.parquet").schema());
    singular.views.addAll(singularViews());
    for (final Source s : new Source[] {names, singular, edge}) {
      for (final String[] v : s.views) {
        runView(spark, s, v);
      }
    }
    spark.stop();
  }
}
