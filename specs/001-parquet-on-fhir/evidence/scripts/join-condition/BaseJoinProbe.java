import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

/** Usage: JoinProbe <dataDir> <scratchDir> [trace]. */
public class BaseJoinProbe {
  static boolean trace;
  static void run(String label, java.util.function.Supplier<Long> s) {
    try { System.out.println(label + " -> " + s.get()); }
    catch (Throwable t) {
      Throwable r = t; while (r.getCause() != null && r.getCause() != r) r = r.getCause();
      System.out.println(label + " -> ERROR " + t.getClass().getSimpleName() + ": "
          + String.valueOf(t.getMessage()).split("\n")[0] + " | root " + r.getClass().getSimpleName());
      if (trace) { t.printStackTrace(System.out); trace = false; }
    }
  }

  public static void main(String[] args) throws Exception {
    trace = args.length > 2;
    final SparkSession spark = SparkSession.builder().master("local[2]")
        .config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    // Raw Spark, no FHIR.
    Dataset<Row> a = spark.sql("select * from values (1,'m'),(2,'f') as a(aid, x)");
    Dataset<Row> b = spark.sql("select * from values (1,'u'),(2,'v') as b(bid, y)");
    Column key = a.col("aid").equalTo(b.col("bid"));
    run("raw join, plain col(x) in condition", () -> a.join(b, col("x").equalTo(lit("m")).and(key)).count());
    final PathlingContext pc = PathlingContext.create(spark);
    for (String layout : List.of("prev")) {
      DatasetSource ds = pc.read().datasets();
      for (String t : List.of("Patient", "Encounter", "Observation")) {
        Dataset<Row> df;
        if (layout.equals("prev")) {
          df = pc.read().ndjson(args[0]).read(t);
        } else {
          Object defs = Class.forName("au.csiro.pathling.definition.fhir.FhirDefinitionContext")
              .getMethod("of", ca.uhn.fhir.context.FhirContext.class).invoke(null, pc.getFhirContext());
          Class<?> rt = Class.forName("au.csiro.pathling.io.transform.ResourceTransformer");
          Method of = null; for (Method m : rt.getMethods()) if (m.getName().equals("of") && m.getParameterCount() == 1) of = m;
          Object tr = of.invoke(null, defs);
          Class<?> rc = Class.forName("au.csiro.pathling.io.json.FhirJsonReader");
          Object reader = rc.getMethod("of", SparkSession.class, rt).invoke(null, spark, tr);
          @SuppressWarnings("unchecked") Dataset<Row> d = (Dataset<Row>) rc.getMethod("read", String.class, String.class)
              .invoke(reader, t, args[0] + "/" + t + ".ndjson");
          df = d;
        }
        String p = args[1] + "/" + layout + "/" + t;
        df.write().mode("overwrite").parquet(p);
        ds.dataset(t, spark.read().parquet(p));
      }
      Dataset<Row> pat = ds.read("Patient");
      Dataset<Row> enc = ds.read("Encounter").select("subject", "status");
      Dataset<Row> obs = ds.read("Observation");
      Column pk = enc.col("subject.reference").endsWith(pat.col("id"));
      for (String e : List.of("gender = 'male'", "name.family.exists()", "birthDate > @1950-01-01",
          "extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race').exists()",
          "deceasedBoolean.empty()", "photo.empty()", "true")) {
        Column c = pc.fhirPathToColumn("Patient", e);
        run(layout + " join cond [" + e + "]", () -> enc.join(pat, c.and(pk)).count());
        run(layout + " join-then-filter [" + e + "]", () -> enc.join(pat, pk).filter(c).count());
      }
      Column g = pc.fhirPathToColumn("Patient", "gender = 'male'");
      Dataset<Row> p1 = pat.alias("p1"), p2 = pat.alias("p2");
      Column selfKey = col("p1.id").equalTo(col("p2.id"));
      run(layout + " self-join cond [gender = 'male']", () -> p1.join(p2, g.and(selfKey)).count());
      run(layout + " self-join-then-filter [gender = 'male']", () -> p1.join(p2, selfKey).filter(g).count());
      Dataset<Row> encFull = ds.read("Encounter");
      Column fk = encFull.col("subject.reference").endsWith(pat.col("id"));
      Column idc = pc.fhirPathToColumn("Patient", "id.exists()");
      run(layout + " join cond, name on both sides [id.exists()]", () -> encFull.join(pat, idc.and(fk)).count());
      Column tc = pc.fhirPathToColumn("Patient", "text.exists()");
      run(layout + " join cond, absent on both sides after select [text.exists()]", () -> enc.join(pat.select("id", "gender"), not(tc).and(pk)).count());
      run(layout + " dropped column: select(id).filter(gender = 'male')", () -> pat.select("id").filter(g).count());
      Column ok = enc.col("subject.reference").equalTo(obs.col("subject.reference"));
      Column q = pc.fhirPathToColumn("Observation", "valueQuantity.value > 50");
      run(layout + " join cond Observation [valueQuantity.value > 50]", () -> enc.limit(50).join(obs, q.and(ok)).count());
      run(layout + " join-then-filter Observation [valueQuantity.value > 50]", () -> enc.limit(50).join(obs, ok).filter(q).count());
    }
    spark.stop();
  }
}
