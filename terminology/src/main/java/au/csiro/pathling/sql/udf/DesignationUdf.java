/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.CodingHelpers.codingEquals;
import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Designation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

/**
 * The implementation of the 'designation()' udf.
 */
@Slf4j
public class DesignationUdf implements SqlFunction,
    SqlFunction3<Row, Row, String, String[]> {

  private static final long serialVersionUID = -4123584679085357391L;

  public static final String FUNCTION_NAME = "designation";
  public static final DataType RETURN_TYPE = DataTypes.createArrayType(DataTypes.StringType);
  public static final String DESIGNATION_PROPERTY_CODE = Designation.PROPERTY_CODE;

  private static final String[] EMPTY_RESULT = new String[0];

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  DesignationUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }

  @Nullable
  protected String[] doCall(@Nullable final Coding coding,
      @Nullable final Coding use, @Nullable final String language) {
    if (coding == null) {
      return null;
    }
    if (!isValidCoding(coding) || (nonNull(use) && !isValidCoding(use))) {
      return EMPTY_RESULT;
    }
    final TerminologyService terminologyService = terminologyServiceFactory.build();
    return terminologyService.lookup(coding, DESIGNATION_PROPERTY_CODE).stream()
        .filter(Designation.class::isInstance)
        .map(Designation.class::cast)
        .filter(designation -> isNull(language) || language.equals(designation.getLanguage()))
        .filter(designation -> isNull(use) || codingEquals(use, designation.getUse()))
        .map(Designation::getValue)
        .toArray(String[]::new);
  }

  @Nullable
  @Override
  public String[] call(@Nullable final Row codingRow, @Nullable final Row use,
      @Nullable final String language) {
    return doCall(decode(codingRow), decode(use), language);
  }
}
