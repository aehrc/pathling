/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import au.csiro.pathling.test.helpers.SparkHelpers;
import ca.uhn.fhir.context.FhirContext;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public class ResourcePathBuilder {

  @Nonnull
  private FhirContext fhirContext;

  @Nonnull
  private ResourceReader resourceReader;

  @Nonnull
  private ResourceType resourceType;

  @Nonnull
  private String expression;

  private boolean singular;

  public ResourcePathBuilder() {
    fhirContext = mock(FhirContext.class);
    resourceReader = mock(ResourceReader.class);
    final SparkSession spark = SparkHelpers.getSparkSession();
    final Dataset<Row> dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    when(resourceReader.read(any(ResourceType.class))).thenReturn(dataset);
    resourceType = ResourceType.PATIENT;
    expression = "";
    singular = false;
  }

  @Nonnull
  public ResourcePathBuilder fhirContext(final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder resourceReader(final ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder resourceType(final ResourceType resourceType) {
    this.resourceType = resourceType;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder expression(final String expression) {
    this.expression = expression;
    return this;
  }

  @Nonnull
  public ResourcePathBuilder singular(final boolean singular) {
    this.singular = singular;
    return this;
  }

  @Nonnull
  public ResourcePath build() {
    return ResourcePath.build(fhirContext, resourceReader, resourceType, expression, singular);
  }
}
