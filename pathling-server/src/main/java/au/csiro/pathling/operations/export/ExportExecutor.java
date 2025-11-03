package au.csiro.pathling.operations.export;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Performs the $export logic.
 *
 * @author Felix Naumann
 */
@Slf4j
@Component
public class ExportExecutor {

  private final PathlingContext pathlingContext;
  private final QueryableDataSource deltaLake;
  private final FhirContext fhirContext;
  private final SparkSession sparkSession;
  private final String databasePath;
  private final ServerConfiguration serverConfiguration;

  @Autowired
  public ExportExecutor(PathlingContext pathlingContext, QueryableDataSource deltaLake,
      FhirContext fhirContext,
      SparkSession sparkSession,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
      String databasePath, ServerConfiguration serverConfiguration) {
    this.pathlingContext = pathlingContext;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Perform the $export request. The input data is already parsed and validated.
   *
   * @param exportRequest The export request data.
   * @param jobId The job id to which this async request belongs to.
   * @return The export response data.
   */
  public ExportResponse execute(ExportRequest exportRequest, String jobId) {
    // filter out resources (silently) due to insufficient permissions
    QueryableDataSource mapped = checkResourceAccess(AccessType.READ, deltaLake);

    mapped = applyResourceTypeFiltering(exportRequest, mapped);
    mapped = applySinceDateFilter(exportRequest, mapped);
    mapped = applyUntilDateFilter(exportRequest, mapped);

    if (!exportRequest.elements().isEmpty()) {
      mapped = applyElementsParams(exportRequest, mapped);
      // If the returned ndjson is limited by the _elements param, then it should have the SUBSETTED tag
      Column subsettedTagArray = createSubsettedTagInSparkStructure();
      mapped = addSubsettedTag(mapped, subsettedTagArray);
    }
    return writeResultToJobDirectory(exportRequest, jobId, mapped);
  }

  private @NotNull QueryableDataSource checkResourceAccess(AccessType accessType,
      QueryableDataSource dataSource) {
    if (!serverConfiguration.getAuth().isEnabled()) {
      return dataSource;
    }
    return dataSource.filterByResourceType(resourceType -> {
      if (!SecurityAspect.hasAuthority(
          PathlingAuthority.resourceAccess(accessType, ResourceType.fromCode(resourceType)))) {
        log.debug("Insufficient {} resource access permissions for {}. Hiding resource from user.",
            accessType.getCode(), resourceType);
        return false;
      } else {
        return true;
      }
    });
  }

  private @NotNull ExportResponse writeResultToJobDirectory(ExportRequest exportRequest,
      String jobId,
      QueryableDataSource mapped) {
    URI warehouseUri = URI.create(databasePath);
    Path warehousePath = new Path(warehouseUri);
    Path jobDirPath = new Path(new Path(warehousePath, "jobs"), jobId);
    Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
    try {
      FileSystem fs = FileSystem.get(configuration);
      if (!fs.exists(jobDirPath)) {
        boolean created = fs.mkdirs(jobDirPath);
        if (!created) {
          throw new InternalErrorException("Failed to created subdirectory at %s for job %s."
              .formatted(databasePath, jobId));
        }
        log.debug("Created dir {}", jobDirPath);
      }

      WriteDetails writeDetails = new DataSinkBuilder(pathlingContext,
          mapped).saveMode("overwrite").ndjson(jobDirPath.toString());
      return new ExportResponse(exportRequest.originalRequest(), writeDetails);
    } catch (IOException e) {
      throw new InternalErrorException("Failed to created subdirectory at %s for job %s."
          .formatted(databasePath, jobId));
    }
  }

  private static @NotNull QueryableDataSource addSubsettedTag(QueryableDataSource mapped,
      Column subsettedTagArray) {
    mapped = mapped.map(rowDataset -> rowDataset.withColumn("meta",
        struct(
            coalesce(col("meta.id"), lit(null).cast(DataTypes.StringType)).as("id"),
            coalesce(col("meta.versionId"), lit(null).cast(DataTypes.StringType)).as("versionId"),
            coalesce(col("meta.versionId_versioned"), lit(null).cast(DataTypes.StringType)).as(
                "versionId_versioned"),
            coalesce(col("meta.lastUpdated"), lit(null).cast(DataTypes.TimestampType)).as(
                "lastUpdated"),
            coalesce(col("meta.source"), lit(null).cast(DataTypes.StringType)).as("source"),
            coalesce(col("meta.profile"),
                lit(null).cast(DataTypes.createArrayType(DataTypes.StringType))).as("profile"),
            coalesce(col("meta.security"), lit(null).cast(
                DataTypes.createArrayType(DataTypes.createStructType(new StructField[]{
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
    return mapped;
  }

  private static Column createSubsettedTagInSparkStructure() {
    return array(struct(
        lit(null).cast(DataTypes.StringType).as("id"),
        lit("http://terminology.hl7.org/CodeSystem/v3-ObservationValue").as("system"),
        lit(null).cast(DataTypes.StringType).as("version"),
        lit("SUBSETTED").as("code"),
        lit("Resource encoded in summary mode").as("display"),
        lit(null).cast(DataTypes.BooleanType).as("userSelected"),
        lit(null).cast(DataTypes.IntegerType).as("_fid")
    ));
  }

  private QueryableDataSource applyElementsParams(ExportRequest exportRequest,
      QueryableDataSource mapped) {
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
    Map<String, UnaryOperator<Dataset<Row>>> localGlobalCombined = localElements.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> {
              Set<String> allElementsForThisResourceType = new HashSet<>(entry.getValue());
              allElementsForThisResourceType.addAll(getMandatoryElements(
                  Enumerations.ResourceType.fromCode(
                      entry.getKey()))); // add all local mandatory elements to be returned
              allElementsForThisResourceType.addAll(globalElements);
              return rowDataset -> rowDataset.select(
                  columnsWithNullification(rowDataset, allElementsForThisResourceType));
            }
        ));
    mapped = mapped.map((resourceType, rowDataset) -> localGlobalCombined.getOrDefault(resourceType,
        UnaryOperator.identity()).apply(rowDataset));

    // Apply global elements to all other resource types that don't have specific elements
    if (!globalElements.isEmpty()) {
      mapped = mapped.map((resourceType, dataset) -> {
        // Only apply if this resource type wasn't already handled by bulkMap
        if (!localGlobalCombined.containsKey(resourceType)) {
          return dataset.select(columnsWithNullification(dataset, globalElements));
        }
        return dataset;
      });
    }
    return mapped;
  }

  private static QueryableDataSource applyUntilDateFilter(ExportRequest exportRequest,
      QueryableDataSource mapped) {
    if (exportRequest.until() != null) {
      mapped = mapped.map(rowDataset -> rowDataset.filter(
          "meta.lastUpdated IS NULL OR meta.lastUpdated <= '" + exportRequest.until()
              .getValueAsString() + "'"));
    }
    return mapped;
  }

  private static QueryableDataSource applySinceDateFilter(ExportRequest exportRequest,
      QueryableDataSource mapped) {
    if (exportRequest.since() != null) {
      mapped = mapped.map(rowDataset -> rowDataset.filter(
          "meta.lastUpdated IS NULL OR meta.lastUpdated >= '" + exportRequest.since()
              .getValueAsString() + "'"));
    }
    return mapped;
  }

  private @NotNull QueryableDataSource applyResourceTypeFiltering(ExportRequest exportRequest,
      QueryableDataSource mapped) {
    // Assume that every resource from the _type param is accessible
    Map<String, Boolean> perResourceAuth = exportRequest.includeResourceTypeFilters().stream()
        .collect(Collectors.toMap(ResourceType::toCode, resourceType -> true));
    if (serverConfiguration.getAuth().isEnabled()) {
      // Provide actual authority access for each resource type
      exportRequest.includeResourceTypeFilters().stream()
          .map(resourceType -> Map.entry(resourceType,
              PathlingAuthority.resourceAccess(AccessType.READ, resourceType)))
          .forEach(entry -> {
            // handling=strict and auth exists -> throw error on wrong auth
            if (!exportRequest.lenient()) {
              SecurityAspect.checkHasAuthority(entry.getValue());
            } else {
              perResourceAuth.put(entry.getKey().toCode(),
                  SecurityAspect.hasAuthority(entry.getValue()));
            }
          });
    }
    // Apply the perResourceAuth map
    return mapped.filterByResourceType(resourceType -> {
      if (exportRequest.includeResourceTypeFilters().isEmpty()) {
        // It is ok to just pass the resources on without further auth, because at this stage,
        // the delta lake only contains resources that the user is allowed to see (it was filtered earlier)
        return true;
      }
      return perResourceAuth.getOrDefault(resourceType, false)
          && exportRequest.includeResourceTypeFilters()
          .contains(Enumerations.ResourceType.fromCode(resourceType));
    });
  }

  private Column[] columnsWithNullification(Dataset<Row> dataset, Set<String> columnsToKeep) {
    return Arrays.stream(dataset.columns())
        .map(colName -> {
          if (columnsToKeep.contains(colName)) {
            return col(colName);
          } else {
            DataType expectedType = dataset.schema().apply(colName).dataType();
            // For MapType, create an empty map instead of null to avoid NullPointerException
            // when Spark tries to convert null Scala Maps to Java Maps.
            if (expectedType instanceof MapType) {
              return map().cast(expectedType).as(colName);
            } else {
              return lit(null).cast(expectedType).as(colName);
            }
          }
        })
        .toArray(Column[]::new);
  }

  public Set<String> getMandatoryElements(Enumerations.ResourceType resourceType) {
    Set<String> alwaysMandatory = Set.of("id");

    RuntimeResourceDefinition resourceDef = fhirContext.getResourceDefinition(
        resourceType.toCode());
    Set<String> mandatoryElements = new HashSet<>();

    for (BaseRuntimeChildDefinition child : resourceDef.getChildren()) {
      if (child.getMin() > 0) {
        mandatoryElements.add(child.getElementName());
      }
    }

    mandatoryElements.addAll(alwaysMandatory);
    return mandatoryElements;
  }
}
