import org.apache.spark.sql.SparkSession;

/**
 * Prints each optimizer rule that changes the plan of the inline wrapper query, through
 * spark.sql.planChangeLog. Usage: Mechanism dataDir.
 */
public class Mechanism {
  public static void main(final String[] a) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    spark.read().parquet(a[0] + "/Patient.parquet").createOrReplaceTempView("t");
    spark.conf().set("spark.sql.planChangeLog.level", "WARN");
    final String s = "named_struct('value', birthDate, 'meta', _birthDate)";
    final String q = "select inline(array(struct(array(" + s + ".value) AS r))) from t";
    System.out.println("QUERY START");
    System.out.println(spark.sql(q).queryExecution().optimizedPlan());
    System.out.println("QUERY END");
    spark.stop();
  }
}
