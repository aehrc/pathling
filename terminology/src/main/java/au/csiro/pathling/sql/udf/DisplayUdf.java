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

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.decode;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.isValidCoding;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;

/**
 * The implementation of the 'display()' udf.
 */
@Slf4j
public class DisplayUdf implements SqlFunction,
    SqlFunction2<Row, String, String> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String DISPLAY_PROPERTY_CODE = "display";
  public static final String FUNCTION_NAME = "display";
  public static final DataType RETURN_TYPE = DataTypes.StringType;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  DisplayUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  protected String doCall(@Nullable final Coding coding, @Nullable final String acceptLanguage) {
    if (coding == null || !isValidCoding(coding)) {
      return null;
    }
    final TerminologyService terminologyService = terminologyServiceFactory.build();
    final List<PropertyOrDesignation> result = terminologyService.lookup(
        coding, DISPLAY_PROPERTY_CODE, acceptLanguage);

    final Optional<Property> maybeDisplayName = result.stream()
        .filter(s -> s instanceof Property)
        .map(s -> (Property) s)
        .filter(p -> DISPLAY_PROPERTY_CODE.equals(p.getCode()))
        .findFirst();

    return maybeDisplayName.map(Property::getValueAsString).orElse(null);
  }

  @Nullable
  @Override
  public String call(@Nullable final Row codingRow, @Nullable final String acceptLanguage) {
    return doCall(decode(codingRow), acceptLanguage);
  }
}
