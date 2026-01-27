/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhir;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.PropertyUtils;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/** Helper functions for dealing with FHIR {@link Parameters} resource. */
public final class ParametersUtils {

  /** The parameter name for property parts. */
  public static final String PROPERTY_PART_NAME = "property";

  /** The parameter name for designation parts. */
  public static final String DESIGNATION_PART_NAME = "designation";

  private ParametersUtils() {
    // Utility class
  }

  @SuppressWarnings("unchecked")
  private static void setProperty(
      @Nonnull final Object bean, @Nonnull final String name, @Nullable final Object value) {
    try {
      final PropertyDescriptor descriptor = PropertyUtils.getPropertyDescriptor(bean, name);
      if (descriptor != null) {
        final Object currentValue = descriptor.getReadMethod().invoke(bean);
        if (currentValue == null) {
          if (List.class.isAssignableFrom(descriptor.getPropertyType())) {
            final List<Object> newList = new ArrayList<>();
            newList.add(value);
            descriptor.getWriteMethod().invoke(bean, newList);
          } else {
            descriptor.getWriteMethod().invoke(bean, value);
          }
        } else if (List.class.isAssignableFrom(descriptor.getPropertyType())) {
          ((List<Object>) currentValue).add(value);
        } else {
          throw new IllegalStateException("Overwriting value of singular property: " + name);
        }
      }
    } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts ParametersParameterComponent object to a Java bean class initializing its properties
   * from the part properties.
   *
   * @param component the ParametersParameterComponent element.
   * @param supplier the supplier for the bean class.
   * @param <T> the type of the bean.
   * @return the new java bean of type T initialize from the ParametersParameterComponent element.
   */
  @Nonnull
  public static <T> T partsToBean(
      @Nonnull final ParametersParameterComponent component, @Nonnull final Supplier<T> supplier) {
    final T result = supplier.get();
    for (final ParametersParameterComponent p : component.getPart()) {
      if (p.hasValue()) {
        setProperty(result, p.getName(), p.getValue());
      } else if (p.hasPart()) {
        setProperty(result, p.getName(), partsToBean(p, supplier));
      }
    }
    return result;
  }

  /**
   * Extracts the boolean value of the 'result' parameter.
   *
   * @param parameters the parameters to convert.
   * @return the boolean value of the 'result' parameter.
   */
  public static boolean toBooleanResult(final @Nonnull Parameters parameters) {
    return parameters.getParameterBool("result");
  }

  /**
   * Extracts the {@link ConceptSubsumptionOutcome} value of the 'outcome' parameter.
   *
   * @param parameters the parameters to convert.
   * @return the {@link ConceptSubsumptionOutcome} value.
   */
  @Nonnull
  public static ConceptSubsumptionOutcome toSubsumptionOutcome(
      final @Nonnull Parameters parameters) {
    return ConceptSubsumptionOutcome.fromCode(
        parameters.getParameter("outcome").getValue().toString());
  }

  /** Object representation of a 'match' part from 'translate()' result. */
  @Data
  @NoArgsConstructor
  public static class MatchPart {

    @Nonnull private Coding concept;

    @Nonnull private CodeType equivalence;
  }

  @Nonnull
  private static MatchPart componentToMatchPart(
      @Nonnull final Parameters.ParametersParameterComponent component) {
    return ParametersUtils.partsToBean(component, MatchPart::new);
  }

  /**
   * Extracts 'match' parts from the result of 'translate()'.
   *
   * @param parameters the parameters to convert.
   * @return the stream of 'match' parts.
   */
  @Nonnull
  public static Stream<MatchPart> toMatchParts(final @Nonnull Parameters parameters) {
    return toBooleanResult(parameters)
        ? parameters.getParameter().stream()
            .filter(pc -> "match".equals(pc.getName()))
            .map(ParametersUtils::componentToMatchPart)
        : Stream.empty();
  }

  /**
   * Object representation of the 'property' part from $lookup results.
   *
   * <p>The fields of this class must match the names of the properties in the response to the
   * lookup operation.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PropertyPart {

    @Nonnull CodeType code;

    @Nullable Type value;

    @SuppressWarnings("SpellCheckingInspection")
    @Nullable
    List<PropertyPart> subproperty;
  }

  @Nullable
  private static PropertyPart toProperty(@Nonnull final ParametersParameterComponent component) {
    if (!component.hasPart()) {
      return new PropertyPart(new CodeType(component.getName()), component.getValue(), null);
    } else if (PROPERTY_PART_NAME.equals(component.getName())) {
      return partsToBean(component, PropertyPart::new);
    } else {
      return null;
    }
  }

  /**
   * Extracts 'property' parts from the result of '$lookup'.
   *
   * @param parameters the parameters to convert.
   * @return the stream of 'property' parts.
   */
  @Nonnull
  public static Stream<PropertyPart> toProperties(@Nonnull final Parameters parameters) {

    return parameters.getParameter().stream()
        .map(ParametersUtils::toProperty)
        .filter(Objects::nonNull);
  }

  /** Object representation of the 'designation' part from $lookup results. */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DesignationPart {

    @Nullable CodeType language;

    @Nullable Coding use;

    @Nonnull StringType value;
  }

  /**
   * Extracts 'designation' parts from the result of '$lookup'.
   *
   * @param parameters the parameters to convert.
   * @return the stream of 'designation' parts.
   */
  @Nonnull
  public static Stream<DesignationPart> toDesignations(@Nonnull final Parameters parameters) {
    return parameters.getParameter().stream()
        .filter(c -> c.hasPart() && DESIGNATION_PART_NAME.equals(c.getName()))
        .map(c -> partsToBean(c, DesignationPart::new));
  }
}
