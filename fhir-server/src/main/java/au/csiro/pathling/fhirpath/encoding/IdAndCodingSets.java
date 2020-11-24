/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a resource identity along with a set of input codings, and a set of argument codings.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private List<SimpleCoding> inputCodings;

  @Nullable
  private List<SimpleCoding> argCodings;

  // public IdAndCodingSets(@Nonnull final Row inputRow) {
  //   final int idIndex = inputRow.length() - 3;
  //   final int inputCodingsIndex = inputRow.length() - 2;
  //   final int argCodingsIndex = inputRow.length() - 1;
  //   id = inputRow.getString(idIndex);
  //   inputRow.getList(inputCodingsIndex).stream()
  //       .map(item -> {
  //         check(item instanceof Row);
  //         final Row row = (Row) item;
  //         return new SimpleCoding(row.fieldIndex());
  //       })
  // }

  /**
   * @return a list of {@link SimpleCoding} objects, or an empty list
   */
  @Nonnull
  public List<SimpleCoding> safeGetInputCodings() {
    return getInputCodings() != null
           ? getInputCodings()
           : Collections.emptyList();
  }

  /**
   * @return a list of {@link SimpleCoding} objects, or an empty list
   */
  @Nonnull
  public List<SimpleCoding> safeGetArgCodings() {
    return getArgCodings() != null
           ? getArgCodings()
           : Collections.emptyList();
  }

}
