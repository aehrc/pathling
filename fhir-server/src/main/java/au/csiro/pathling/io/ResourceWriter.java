/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.fileNameForResource;

import au.csiro.pathling.Configuration;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
@Component
public class ResourceWriter {

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  /**
   * @param configuration A {@link Configuration} object which controls the behaviour of the writer
   */
  public ResourceWriter(@Nonnull final Configuration configuration) {
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getWarehouseUrl());
    this.databaseName = configuration.getDatabaseName();
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied {@link
   * Dataset}.
   *
   * @param resourceType The type of the resource to write.
   * @param resources The {@link Dataset} containing the resource data.
   */
  public void write(@Nonnull final ResourceType resourceType, @Nonnull final Dataset resources) {
    final String tableUrl =
        warehouseUrl + "/" + databaseName + "/" + fileNameForResource(resourceType);
    resources.write().mode(SaveMode.Overwrite).parquet(tableUrl);
  }

}
