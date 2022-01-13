/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.fileNameForResource;
import static org.apache.spark.sql.functions.asc;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
public class ResourceWriter {

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  /**
   * @param configuration A {@link Configuration} object which controls the behaviour of the writer
   */
  public ResourceWriter(@Nonnull final Configuration configuration) {
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());
    this.databaseName = configuration.getStorage().getDatabaseName();
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied {@link
   * Dataset}.
   *
   * @param resourceType The type of the resource to write.
   * @param resources The {@link Dataset} containing the resource data.
   */
  @ResourceAccess(AccessType.WRITE)
  public void write(@Nonnull final ResourceType resourceType, @Nonnull final Dataset resources) {
    final String tableUrl =
        warehouseUrl + "/" + databaseName + "/" + fileNameForResource(resourceType);
    // We order the resources here to reduce the amount of sorting necessary at query time.
    resources.orderBy(asc("id"))
        .write()
        .mode(SaveMode.Overwrite)
        .format("delta")
        .save(tableUrl);
  }

}
