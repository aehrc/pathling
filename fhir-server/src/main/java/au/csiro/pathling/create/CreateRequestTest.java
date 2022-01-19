package au.csiro.pathling.create;

import jodd.io.filter.WildcardFileFilter;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.util.Objects;

public class CreateRequestTest {
    @Test
    public void DeltaBuilder() {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.host", "localhost")
                .getOrCreate();

        final String sourcePath = "src/main/java/au/csiro/pathling/create/inputfiles/";
        final String targetPath = "src/main/java/au/csiro/pathling/create/tmp/delta-table/";
        final File srcNdJsonDir = new File(sourcePath);
        final FileFilter fileFilter = new WildcardFileFilter("*.ndjson");
        final File[] srcNdJsonFiles = srcNdJsonDir.listFiles(fileFilter);

        for (final File srcFile : Objects.requireNonNull(srcNdJsonFiles)) {
            System.out.println("Loading source NDJSON file: " + srcFile);
            final String resourceName = FilenameUtils.getBaseName(srcFile.getName());
            final Enumerations.ResourceType subjectResource = Enumerations.ResourceType
                    .valueOf(resourceName.toUpperCase());
            final Dataset<String> jsonStrings = spark.read().textFile(srcFile.getPath());
            jsonStrings.show();
            jsonStrings.write()
                    .format("delta")
                    .save(targetPath + subjectResource.toCode());
            System.out.println(subjectResource.toCode());
        }
    }

    @Test
    public void DeltaReader() {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.host", "localhost")
                .getOrCreate();

        final String sourcePath = "src/main/java/au/csiro/pathling/create/tmp/delta-table/DiagnosticReport";

        Dataset<Row> df = spark.read().format("delta").load(sourcePath);
        df.show(false);

    }
}