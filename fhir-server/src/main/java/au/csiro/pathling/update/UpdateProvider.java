/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.server.IResourceProvider;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * HAPI resource provider that provides update-related operations, such as create, update and
 * transaction.
 *
 * @author John Grimes
 */
@Component
@Scope("prototype")
@Profile("server")
public class UpdateProvider implements IResourceProvider {

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceWriter resourceWriter;

  @Nonnull
  private final Class<? extends IBaseResource> resourceClass;

  @Nonnull
  private final ResourceType resourceType;

  @Nonnull
  private final CacheInvalidator cacheInvalidator;

  /**
   * @param resourceClass the resource class that this provider will receive requests for
   */
  public UpdateProvider(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ResourceWriter resourceWriter,
      @Nonnull final CacheInvalidator cacheInvalidator,
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.resourceWriter = resourceWriter;
    this.resourceClass = resourceClass;
    this.cacheInvalidator = cacheInvalidator;
    resourceType = resourceTypeFromClass(resourceClass);
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return resourceClass;
  }

  @Create
  @OperationAccess("create")
  public MethodOutcome create(@ResourceParam final IBaseResource resource) {
    resource.setId(UUID.randomUUID().toString());

    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<IBaseResource> dataset = spark.createDataset(List.of(resource), encoder);
    resourceWriter.append(resourceType, dataset);

    cacheInvalidator.invalidateAll();

    final MethodOutcome outcome = new MethodOutcome();
    outcome.setId(resource.getIdElement());
    outcome.setResource(resource);
    return outcome;
  }

}
