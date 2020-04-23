package au.csiro.pathling;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.FhirContextFactory;
import ca.uhn.fhir.context.FhirVersionEnum;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;

import java.io.File;
import java.io.FileFilter;

/**
 * Converts the test fhir data in `src/test/resources/test-data/fhir` to
 * their parquet version in `src/test/resources/test-data/parquet`.
 * TODO: For now it needs to be run manually. In the future integrate with the build somehow
 */
public class TestDataFhir2ParquetApp {

    protected SparkSession spark;
    protected FhirEncoders fhirEncoders = FhirEncoders.forR4().getOrCreate();

    public static void main(String[] args) throws Exception {
        new TestDataFhir2ParquetApp().run(args);
    }

    public void setUp() throws Exception {
        spark = SparkSession.builder()
                .appName("pathling-test")
                .config("spark.master", "local[*]")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    public void run(String[] args) throws Exception {
        setUp();
        final File srcNdJsonDir = new File("src/test/resources/test-data/fhir");
        final FileFilter fileFilter = new WildcardFileFilter("*.ndjson");
        File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);
        for(File srcFile: srcNdJsonFiles) {
            String resourceName = FilenameUtils.getBaseName(srcFile.getName());
            Enumerations.ResourceType subjectResource = Enumerations.ResourceType.valueOf(resourceName.toUpperCase());
            Dataset<String> jsonStrings = spark.read().textFile(srcFile.getPath());
            final ExpressionEncoder<IBaseResource> fhirEncoder = fhirEncoders.of(subjectResource.toCode());
            final FhirContextFactory localFhirContextFactory = new FhirContextFactory(FhirVersionEnum.R4);
            Dataset<IBaseResource> resourcesDataset = jsonStrings
                    .map((MapFunction<String, IBaseResource>) json -> localFhirContextFactory
                            .build().newJsonParser().parseResource(json), fhirEncoder);
            String outputParquet = "src/test/resources/test-data/parquet/" + subjectResource.toCode() + ".parquet";
            resourcesDataset.write().mode(SaveMode.Overwrite).parquet(outputParquet);
        }
    }

}
