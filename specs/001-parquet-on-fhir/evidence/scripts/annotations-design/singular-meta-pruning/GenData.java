import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Writes N extension-heavy Patients to Parquet in the stored layout. Usage: GenData out N. Every
 * row has birthDate and active; 2 in 3 have deceasedDateTime. Even-numbered rows carry five string
 * extensions on _birthDate and on _deceasedDateTime (the latter where the row has the value).
 */
public class GenData {

  static final String EXT_TYPE = "array<struct<url:string,valueString:string>>";

  /** Rebuilds the _x group with an id and extension array beside the stored annotations. */
  static String withExtensions(final String col) {
    final String exts =
        "transform(sequence(1,5), k -> named_struct('url', concat('http://example.org/ext/e', k),"
            + " 'valueString', concat('payload-', substring(id,2), '-', k,"
            + " '-', md5(concat(id, k)), md5(concat(k, id)))))";
    return "if("
        + col
        + " is null, null, named_struct('id', cast(null as string), 'extension',"
        + " if(cast(substring(id,2) as long)%2=0, "
        + exts
        + ", cast(null as "
        + EXT_TYPE
        + ")), '__start', "
        + col
        + ".__start, '__end', "
        + col
        + ".__end))";
  }

  public static void main(final String[] a) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final long n = Long.parseLong(a[1]);
    final Dataset<Row> json =
        spark
            .range(n)
            .selectExpr(
                "concat('{\"resourceType\":\"Patient\",\"id\":\"p', id, '\",\"active\":',"
                    + " if(id%2=0,'true','false'),"
                    + " ',\"birthDate\":\"', date_format(date_add(DATE'1950-01-01',"
                    + " cast(id%25000 as int)),'yyyy-MM-dd'), '\"',"
                    + " if(id%3<2, concat(',\"deceasedDateTime\":\"', date_format(date_add("
                    + "DATE'1990-01-01', cast(id%12000 as int)),'yyyy-MM-dd'),"
                    + " 'T10:00:00Z\"'), ''),"
                    + " '}') as value");
    final PathlingContext pc = PathlingContext.create(spark);
    // The encoder does not yet keep extensions in the _x group, so they are added after encoding,
    // to the groups that hold the stored annotations. Distinct payloads stop dictionary
    // compression from hiding their size.
    final Dataset<Row> enc =
        pc.encode(json.select("value"), "Patient")
            .withColumn("_birthDate", functions.expr(withExtensions("_birthDate")))
            .withColumn("_deceasedDateTime", functions.expr(withExtensions("_deceasedDateTime")));
    enc.write().mode("overwrite").parquet(a[0] + "/Patient.parquet");
    final Dataset<Row> r = spark.read().parquet(a[0] + "/Patient.parquet");
    System.out.println("rows=" + r.count());
    System.out.println(r.schema().apply("_birthDate").dataType().simpleString());
    System.out.println(r.schema().apply("_deceasedDateTime").dataType().simpleString());
    r.selectExpr("birthDate", "_birthDate", "deceasedDateTime", "_deceasedDateTime")
        .show(3, 120, true);
    spark.stop();
  }
}
