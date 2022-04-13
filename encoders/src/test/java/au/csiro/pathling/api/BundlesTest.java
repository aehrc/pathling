package au.csiro.pathling.api;

import static org.junit.Assert.assertEquals;

import au.csiro.pathling.api.Bundles.BundleContainer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BundlesTest {

  private final Bundles bundles = Bundles.forR4();
  private static SparkSession spark;
  private static JavaSparkContext jsc;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .getOrCreate();

     jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  @Test
  public void testLoadJsonBundlesFromDataframe() {
    final Dataset<Row> jsonBundlesDataset = spark
        .createDataset(
            jsc.wholeTextFiles("src/test/resources/data/bundles/R4/json", 5).values().rdd(),
            Encoders.STRING()).toDF();

    final JavaRDD<BundleContainer> bundleContainersRDD = bundles.fromJson(jsonBundlesDataset, "value");
    assertEquals(5, bundleContainersRDD.count());
  }

  @Test
  public void testLoadXmlBundlesFromDataframe() {
    final Dataset<Row> xmlBundlesDataset = spark
        .createDataset(
            jsc.wholeTextFiles("src/test/resources/data/bundles/R4/xml", 5).values().rdd(),
            Encoders.STRING()).toDF();

    final JavaRDD<BundleContainer> bundleContainersRDD = bundles.fromXml(xmlBundlesDataset, "value");
    assertEquals(5, bundleContainersRDD.count());
  }

  @Test
  public void testLoadJsonBundlesFromDirectory() {
    final JavaRDD<BundleContainer> bundlesRDD = bundles
        .loadFromDirectory(spark, "src/test/resources/data/bundles/R4/json", 5);
    assertEquals(5, bundlesRDD.count());
  }

  @Test
  public void testLoadXmlBundlesFromDirectory() {
    final JavaRDD<BundleContainer> bundlesRDD = bundles
        .loadFromDirectory(spark, "src/test/resources/data/bundles/R4/xml", 5);
    assertEquals(5, bundlesRDD.count());
  }


  @Test
  public void testExtractEntryFromBundles() {
    final JavaRDD<BundleContainer> bundlesRDD = bundles
        .loadFromDirectory(spark, "src/test/resources/data/bundles/R4/json", 5);

    final Dataset<Row> patientsDataframe = bundles.extractEntry(spark, bundlesRDD, "Patient");
    assertEquals(5, patientsDataframe.count());

    final Dataset<Row> conditionsDataframe = bundles.extractEntry(spark, bundlesRDD, Condition.class);
    assertEquals(107, conditionsDataframe.count());
  }

  @Test
  public void testExtractEntryFromResources() {
    final Dataset<Row> jsonResources = spark.read()
        .text("src/test/resources/data/resources/R4/json");

    // One bundle per resource
    final JavaRDD<BundleContainer> bundlesRDD = bundles.fromResourceJson(jsonResources, "value");
    assertEquals(1583, bundlesRDD.count());

    final Dataset<Row> patientsDataframe = bundles.extractEntry(spark, bundlesRDD, "Patient");
    assertEquals(9, patientsDataframe.count());

    final Dataset<Row> conditionsDataframe = bundles.extractEntry(spark, bundlesRDD, Condition.class);
    assertEquals(71, conditionsDataframe.count());
  }
}