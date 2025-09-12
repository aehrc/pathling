package au.csiro.pathling.export;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSink;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.collection.Seq;
import scala.collection.TraversableOnce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ExportExecutor {

    private final PathlingContext pathlingContext;
    private final QueryableDataSource deltaLake;
    private final FhirContext fhirContext;
  private final SparkSession sparkSession;
    private String warehouseUrl;

    @Autowired
    public ExportExecutor(PathlingContext pathlingContext, QueryableDataSource deltaLake, FhirContext fhirContext,
        SparkSession sparkSession, @Value("${pathling.storage.warehouseUrl}") String warehouseUrl) {
        this.pathlingContext = pathlingContext;
        this.deltaLake = deltaLake;
        this.fhirContext = fhirContext;
      this.sparkSession = sparkSession;
      this.warehouseUrl = new Path(warehouseUrl, "jobs").toString();
    }

  /**
   * Use for tests only where it does not matter in which subdirectory the ndjson is written to.
   * @param exportRequest Information about the export request.
   * @return Information about the export response.
   */
  @VisibleForTesting
    public ExportResponse execute(ExportRequest exportRequest) {
      return execute(exportRequest, UUID.randomUUID().toString());
    }
    
    public ExportResponse execute(ExportRequest exportRequest, String jobId) {
        QueryableDataSource filtered = deltaLake.filterByResourceType(resourceType -> {
            if(exportRequest.includeResourceTypeFilters().isEmpty()) {
                return true;
            }
            return exportRequest.includeResourceTypeFilters().contains(Enumerations.ResourceType.fromCode(resourceType));
        });
        
        QueryableDataSource mapped = filtered.map(rowDataset -> {
            rowDataset.printSchema(1);
            return rowDataset.filter("meta.lastUpdated IS NULL OR meta.lastUpdated >= '" + exportRequest.since().getValueAsString() + "'");
        });
        if(exportRequest.until() != null) {
            mapped = mapped.map(rowDataset -> rowDataset.filter("meta.lastUpdated IS NULL OR meta.lastUpdated <= '" + exportRequest.until().getValueAsString() + "'"));
        }

        if(!exportRequest.elements().isEmpty()) {
            Map<String, Set<String>> localElements = exportRequest.elements().stream()
                    .filter(fhirElement -> fhirElement.resourceType() != null)
                    .collect(Collectors.groupingBy(
                            fhirElement -> fhirElement.resourceType().toCode(),
                            Collectors.mapping(
                                    ExportRequest.FhirElement::elementName,
                                    Collectors.toSet()
                            )
                    ));
            Set<String> globalElements = exportRequest.elements().stream()
                    .filter(fhirElement -> fhirElement.resourceType() == null)
                    .map(ExportRequest.FhirElement::elementName)
                    .collect(Collectors.toCollection(HashSet::new));
            globalElements.add("id"); // id is globally mandatory
            globalElements.add("id_versioned"); // id_versioned is coupled to id in spark datasets
            globalElements.add("meta"); // meta is globally mandatory
            Map<String, UnaryOperator<Dataset<Row>>> localGlobalCombined = localElements.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> {
                                Set<String> allElementsForThisResourceType = new HashSet<>(entry.getValue());
                                allElementsForThisResourceType.addAll(getMandatoryElements(Enumerations.ResourceType.fromCode(entry.getKey()))); // add all local mandatory elements to be returned
                                allElementsForThisResourceType.addAll(globalElements);
                                return rowDataset -> rowDataset.select(columnsWithNullification(rowDataset, allElementsForThisResourceType));
                                //return rowDataset -> applyColumnNullification(rowDataset, allElementsForThisResourceType);
                            }
                    ));
            mapped = mapped.bulkMap(localGlobalCombined);

            // Apply global elements to all other resource types that don't have specific elements
            if (!globalElements.isEmpty()) {
                mapped = mapped.map((resourceType, dataset) -> {
                    // Only apply if this resource type wasn't already handled by bulkMap
                    if (!localGlobalCombined.containsKey(resourceType)) {
                        return dataset.select(columnsWithNullification(dataset, globalElements));
                        //return applyColumnNullification(dataset, globalElements);
                    }
                    return dataset; // Already transformed by bulkMap
                });
            }

            // If the returned ndjson is limited by the _elements param, then it should have the SUBSETTED tag
            Column subsettedTagArray = array(struct(
                    lit(null).cast(DataTypes.StringType).as("id"),
                    lit("http://terminology.hl7.org/CodeSystem/v3-ObservationValue").as("system"),
                    lit(null).cast(DataTypes.StringType).as("version"),
                    lit("SUBSETTED").as("code"),
                    lit("Resource encoded in summary mode").as("display"),
                    lit(null).cast(DataTypes.BooleanType).as("userSelected"),
                    lit(null).cast(DataTypes.IntegerType).as("_fid")
            ));

            mapped = mapped.map(rowDataset -> rowDataset.withColumn("meta",
                    struct(
                            coalesce(col("meta.id"), lit(null).cast(DataTypes.StringType)).as("id"),
                            coalesce(col("meta.versionId"), lit(null).cast(DataTypes.StringType)).as("versionId"),
                            coalesce(col("meta.versionId_versioned"), lit(null).cast(DataTypes.StringType)).as("versionId_versioned"),
                            coalesce(col("meta.lastUpdated"), lit(null).cast(DataTypes.TimestampType)).as("lastUpdated"),
                            coalesce(col("meta.source"), lit(null).cast(DataTypes.StringType)).as("source"),
                            coalesce(col("meta.profile"), lit(null).cast(DataTypes.createArrayType(DataTypes.StringType))).as("profile"),
                            coalesce(col("meta.security"), lit(null).cast(DataTypes.createArrayType(DataTypes.createStructType(new StructField[]{
                                    DataTypes.createStructField("id", DataTypes.StringType, true),
                                    DataTypes.createStructField("system", DataTypes.StringType, true),
                                    DataTypes.createStructField("version", DataTypes.StringType, true),
                                    DataTypes.createStructField("code", DataTypes.StringType, true),
                                    DataTypes.createStructField("display", DataTypes.StringType, true),
                                    DataTypes.createStructField("userSelected", DataTypes.BooleanType, true),
                                    DataTypes.createStructField("_fid", DataTypes.IntegerType, true)
                            })))).as("security"),
                            // Always combine existing tags with the new SUBSETTED tag
                            array_union(
                                    coalesce(col("meta.tag"), array()),
                                    subsettedTagArray
                            ).as("tag"),
                            coalesce(col("meta._fid"), lit(null).cast(DataTypes.IntegerType)).as("_fid")
                    )
            ));
        }
        URI warehouseUri = URI.create(warehouseUrl);
        Path warehousePath = new Path(warehouseUri);
        Path jobDirPath = new Path(warehousePath, jobId);
        Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
      try {
        FileSystem fs = FileSystem.get(configuration);
        if(!fs.exists(jobDirPath)) {
          boolean created = fs.mkdirs(jobDirPath);
          if(!created) {
            throw new InternalErrorException("Failed to created subdirectory at %s for job %s."
                .formatted(warehouseUrl, jobId));
          }
        }
        DataSink.NdjsonWriteDetails writeDetails = new DataSinkBuilder(pathlingContext, mapped).saveMode("overwrite").ndjson(jobDirPath.toString());
        return new ExportResponse(exportRequest.originalRequest(), writeDetails);
      } catch (IOException e) {
        throw new InternalErrorException("Failed to created subdirectory at %s for job %s."
            .formatted(warehouseUrl, jobId));
      }
    }

    private Dataset<Row> applyColumnNullification(Dataset<Row> dataset, Set<String> columnsToKeep) {
        Dataset<Row> result = dataset;
        for (String colName : dataset.columns()) {
            if (!columnsToKeep.contains(colName)) {
                DataType expectedType = dataset.schema().apply(colName).dataType();
                result = result.withColumn(colName, lit(null).cast(expectedType));
            }
        }
        return result;
    }

    private Column[] columnsWithNullification(Dataset<Row> dataset, Set<String> columnsToKeep) {
        return Arrays.stream(dataset.columns())
                .map(colName -> {
                    if (columnsToKeep.contains(colName)) {
                        return col(colName);
                    } else {
                        DataType expectedType = dataset.schema().apply(colName).dataType();
                        //return createNullColumn(colName, expectedType);
                        return lit(null).cast(expectedType).as(colName);
                        //return lit(null).as(colName);
                    }
                })
                .toArray(Column[]::new);
    }

    public Set<String> getMandatoryElements(Enumerations.ResourceType resourceType) {
        Set<String> alwaysMandatory = Set.of("id");

        RuntimeResourceDefinition resourceDef = fhirContext.getResourceDefinition(resourceType.toCode());
        Set<String> mandatoryElements = new HashSet<>();

        for (BaseRuntimeChildDefinition child : resourceDef.getChildren()) {
            if (child.getMin() > 0) {
                mandatoryElements.add(child.getElementName());
            }
        }

        mandatoryElements.addAll(alwaysMandatory);
        return mandatoryElements;
    }

    public String getWarehouseUrl() {
        return warehouseUrl;
    }

    public void setWarehouseUrl(String warehouseUrl) {
        this.warehouseUrl = warehouseUrl;
    }
}
