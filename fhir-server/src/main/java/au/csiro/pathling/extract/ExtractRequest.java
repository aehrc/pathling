/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents the information provided as part of an invocation of the "extract" operation.
 *
 * @author John Grimes
 */
@Value
public class ExtractRequest {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  List<String> columns;

  @Nonnull
  List<String> filters;

  @Nonnull
  String requestId;

  /**
   * @param subjectResource the resource which will serve as the input context for each expression
   * @param columns a set of columns expressions to execute over the data
   * @param filters the criteria by which the data should be filtered
   * @param requestId an identifier for the request used to initiate this
   */
  public ExtractRequest(@Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<List<String>> columns, @Nonnull final Optional<List<String>> filters,
      @Nonnull final String requestId) {
    checkUserInput(columns.isPresent() && columns.get().size() > 0,
        "Query must have at least one column expression");
    checkUserInput(columns.get().stream().noneMatch(String::isBlank),
        "Column expression cannot be blank");
    filters.ifPresent(f -> checkUserInput(f.stream().noneMatch(String::isBlank),
        "Filter expression cannot be blank"));
    this.subjectResource = subjectResource;
    this.columns = columns.get();
    this.filters = filters.orElse(Collections.emptyList());
    this.requestId = requestId;
  }

}
