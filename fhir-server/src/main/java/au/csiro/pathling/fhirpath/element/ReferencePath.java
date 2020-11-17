/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Referrer;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIRPath expression which is a resource reference.
 *
 * @author John Grimes
 */
public class ReferencePath extends ElementPath implements Referrer {

  protected ReferencePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, foreignResource, thisColumn,
        fhirType);
  }

  @Nonnull
  public Set<ResourceType> getResourceTypes() {
    if (getDefinition().isPresent()) {
      return getDefinition().get().getReferenceTypes();
    } else {
      return Collections.emptySet();
    }
  }

}
