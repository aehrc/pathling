package au.csiro.pathling.api;

import ca.uhn.fhir.context.FhirVersionEnum;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.hl7.fhir.r4.model.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ApiTest {

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
    public void testVersionString() {
        System.out.println(FhirVersionEnum.forVersionString("4.0.2").getFhirVersionString());
    }


    @Test
    public void testEncodeResourcesFromJsonBundle() {

        final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
                .textFile("src/test/resources/data/bundles/R4/json");


        final PathlingContext pathling = PathlingContext.of(spark);

        final Dataset<Row> patientsDataframe = pathling.encodeBundle(bundlesDF, "Patient", FhirMimeTypes.FHIR_JSON);
        assertEquals(5, patientsDataframe.count());

        final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class, FhirMimeTypes.FHIR_JSON);
        assertEquals(107, conditionsDataframe.count());
    }


    @Test
    public void testEncodeResourcesFromXmlBundle() {

        final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
                .textFile("src/test/resources/data/bundles/R4/xml");


        final PathlingContext pathling = PathlingContext.of(spark);
        final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class, FhirMimeTypes.FHIR_XML);
        assertEquals(107, conditionsDataframe.count());
    }


    @Test
    public void testExtractEntryFromResources() {
        final Dataset<String> jsonResources = spark.read()
                .text("src/test/resources/data/resources/R4/json").as(Encoders.STRING());

        final PathlingContext pathling = PathlingContext.of(spark);

        final Dataset<Row> patientsDataframe = pathling.encode(jsonResources, "Patient", FhirMimeTypes.FHIR_JSON);
        assertEquals(9, patientsDataframe.count());

        final Dataset<Condition> conditionsDataframe = pathling.encode(jsonResources, Condition.class, FhirMimeTypes.FHIR_JSON);
        assertEquals(71, conditionsDataframe.count());
    }

    @Test
    public void testEncodeResourceStream() throws Exception {
        final PathlingContext pathling = PathlingContext.of(spark);

        final Dataset<String> jsonResources = spark.readStream()
                .text("src/test/resources/data/resources/R4/json").as(Encoders.STRING());

        assertTrue(jsonResources.isStreaming());

        final Dataset<Row> patientsStream = pathling.encode(jsonResources, "Patient", FhirMimeTypes.FHIR_JSON);

        assertTrue(patientsStream.isStreaming());

        final StreamingQuery patientsQuery = patientsStream
                .writeStream()
                .queryName("patients")
                .format("memory")
                .start();

        patientsQuery.processAllAvailable();
        final long patientsCount = spark.sql("select count(*) from patients").head().getLong(0);
        assertEquals(patientsCount, patientsCount);

        final StreamingQuery conditionQuery = pathling.encode(jsonResources, "Condition", FhirMimeTypes.FHIR_JSON)
                .groupBy()
                .count()
                .writeStream()
                .outputMode(OutputMode.Complete())
                .queryName("countCondition")
                .format("memory")
                .start();

        conditionQuery.processAllAvailable();
        final long conditionsCount = spark.sql("select * from countCondition").head().getLong(0);
        assertEquals(71, conditionsCount);
    }
}