package au.csiro.pathling.api;

import au.csiro.pathling.api.Bundles.BundleContainer;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BundlesTest {

  private static SparkSession spark;

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
  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
  }


  @Test
  public void testLoadBundlesFromDataframe() {

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    final Dataset<Row> jsonBundlesDataset = spark
        .createDataset(
            jsc.wholeTextFiles("src/test/resources/data/bundles/R4/json", 5).values().rdd(),
            Encoders.STRING()).toDF();

    jsonBundlesDataset.printSchema();

    final Bundles bundles = Bundles.forR4();
    JavaRDD<BundleContainer> bundleContainersRDD = bundles.fromJson(jsonBundlesDataset, "value");

    final Dataset<Row> conditionsDataset = bundles
        .extractEntry(spark, bundleContainersRDD, "Observation");

    conditionsDataset.show();
  }


  @Test
  public void testLoadBundlesFromDirectory() {
    final Bundles bundles = Bundles.forR4();
    final JavaRDD<BundleContainer> jsonData = bundles
        .loadFromDirectory(spark, "src/test/resources/data/bundles/R4/json", 5);

    final List<BundleContainer> bundleContainers = jsonData.collect();

    final Dataset<Row> conditionsDataset = bundles
        .extractEntry(spark, jsonData, "Patient");

    System.out.println(conditionsDataset.select("identifier").collectAsList());
    //System.out.println(bundleContainers);
  }

}