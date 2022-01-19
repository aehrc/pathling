package au.csiro.pathling.create;

import au.csiro.pathling.Configuration;
import jodd.io.filter.WildcardFileFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;

@Component
@Profile("core & !ga4gh")
@Slf4j
public class CreateRequest {

    @Nonnull
    private final SparkSession spark;

    public CreateRequest() {
        this.spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Create")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
    }

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        CreateRequest createRequest = new CreateRequest();
        //createRequest.DeltaBuilder();
        //createRequest.DeltaReader();
        createRequest.FileStream();
    }

    public void DeltaBuilder() {
        final String sourcePath = "fhir-server/src/main/java/au/csiro/pathling/create/inputfiles/";
        final String targetPath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/delta/";
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

    public void DeltaReader() {
        final String sourcePath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/delta-table/CarePlan";

        Dataset<Row> df = spark.read().format("delta").load(sourcePath);
        df.show(false);
    }

    public void DeltaOverwrite() {
        final String sourcePath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/delta-table/CarePlanTest";
        final String targetPath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/delta-table/AllergyIntolerance";

        Dataset<Row> df = spark.read().format("delta").load(sourcePath);
        Dataset<Row> dfNew = spark.read().format("delta").load(targetPath);

    }

    public void FileStream() throws TimeoutException, StreamingQueryException {
        final String sourcePath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/inputstream";
        final String checkpointPath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/checkpoint";
        final String targetPath = "fhir-server/src/main/java/au/csiro/pathling/create/tmp/outputstream";

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });

        // next step use delta instead of csv
        Dataset<Row> df = spark.readStream()
                .format("csv")
                .option("header", false)
                .schema(schema)
                .load(sourcePath);

        df.writeStream()
                .format("csv")
                .option("checkpointLocation", checkpointPath)
                .option("path", targetPath)
                .start()
                .awaitTermination();
    }
}