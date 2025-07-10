package au.csiro.pathling.io;

import au.csiro.pathling.encoders.EncoderBuilder;
import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

/**
 * A persistence scheme that stores Delta tables using the Spark catalog.
 *
 * @author John Grimes
 */
public class CatalogPersistence implements PersistenceScheme {

  private final Map<String, ResourceType> LOWER_CASE_RESOURCE_NAMES;

  {
    // Create a map between a case-insensitive representation of the resource type name and the
    // resource type itself.
    final Set<String> unsupported = JavaConverters.setAsJavaSet(
        EncoderBuilder.UNSUPPORTED_RESOURCES());
    LOWER_CASE_RESOURCE_NAMES = Arrays.stream(ResourceType.values())
        .filter(resourceType -> !unsupported.contains(resourceType.name()))
        .filter(resourceType -> !resourceType.equals(ResourceType.NULL))
        .collect(Collectors.toMap(
            resourceType -> resourceType.name().toLowerCase(),
            resourceType -> resourceType
        ));
  }

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final Optional<String> schema;

  public CatalogPersistence(@Nonnull final SparkSession spark,
      @Nonnull final Optional<String> schema) {
    this.spark = spark;
    this.schema = schema;
  }

  @Nonnull
  @Override
  public DeltaTable read(@Nonnull final ResourceType resourceType) {
    return DeltaTable.forName(spark, getTableName(resourceType));
  }

  @Override
  public void write(@Nonnull final ResourceType resourceType,
      @Nonnull final DataFrameWriter<Row> writer) {
    writer.saveAsTable(getTableName(resourceType));
  }

  @Override
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final DeltaMergeBuilder merge) {
    merge.execute();
  }

  @Override
  public boolean exists(@Nonnull final ResourceType resourceType) {
    try {
      DeltaTable.forName(spark, getTableName(resourceType));
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  @Override
  public void invalidate(@Nonnull final ResourceType resourceType) {
    // Do nothing.
  }

  @Nonnull
  @Override
  public Set<ResourceType> list() {
    // Get the list of tables from the catalog, and then filter it to only include those that
    // correspond to a known resource type.
    return getTables().stream()
        .map(Table::name)
        .map(String::toLowerCase)
        .map(LOWER_CASE_RESOURCE_NAMES::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  /**
   * @param resourceType the resource type to get the table name for
   * @return the name of the table for the given resource type, qualified by the specified schema if
   * one is provided
   */
  @Nonnull
  private String getTableName(@Nonnull final ResourceType resourceType) {
    return schema.map(s -> String.join(".", s, resourceType.toCode()))
        .orElseGet(resourceType::toCode);
  }

  /**
   * @return the list of tables from the catalog, using the specified schema if one is provided
   */
  private List<Table> getTables() {
    final Catalog catalog = spark.catalog();
    final Dataset<Table> tablesDataset;
    // If a schema is provided, use it to list the tables. Otherwise, list tables from the currently 
    // selected schema.
    tablesDataset = schema.map(dbName -> {
          try {
            return catalog.listTables(dbName);
          } catch (final AnalysisException e) {
            throw new RuntimeException("Specified schema was not found", e);
          }
        })
        .orElseGet(catalog::listTables);
    return tablesDataset.collectAsList();
  }

  @Override
  public void delete(@Nonnull final ResourceType resourceType) {
    final String tableName = getTableName(resourceType);
    try {
      spark.sql("DROP TABLE IF EXISTS " + tableName);
      log.debug("Deleted table: {}", tableName);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to delete table: " + tableName, e);
    }
  }

}
