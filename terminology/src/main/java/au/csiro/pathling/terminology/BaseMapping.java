/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.util.OperationOutcomeUtil;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;

/**
 * Helper function for request/response mappings.
 *
 * @author Piotr Szul
 */
@Deprecated
public class BaseMapping {

  protected BaseMapping() {
  }

  private static void setProperty(@Nonnull final Object bean, @Nonnull final String name,
      @Nullable final Object value) {
    try {
      BeanUtils.setProperty(bean, name, value);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts ParametersParameterComponent object to a java bean class initializing its properties
   * from the part properties.
   *
   * @param component the ParametersParameterComponent element.
   * @param supplier the supplier for the bean class.
   * @param <T> the type of the bean.
   * @return the new java bean of type T initialize from the ParametersParameterComponent element.
   */
  @Nonnull
  public static <T> T partToBean(@Nonnull final ParametersParameterComponent component,
      @Nonnull final Supplier<T> supplier) {
    final T result = supplier.get();
    component.getPart().forEach(p -> setProperty(result, p.getName(), p.getValue()));
    return result;
  }


  /**
   * Retrieves parameters from successful bundle entry element.
   *
   * @param entry the bundle entry.
   * @param fhirContext the Fhir context to use.
   * @return the parameters from the entry.
   * @throws au.csiro.pathling.errors.UnexpectedResponseException when the entry response code is
   * not 200.
   */
  @Nonnull
  public static Parameters parametersFromEntry(@Nonnull final BundleEntryComponent entry,
      @Nonnull final FhirContext fhirContext) {
    checkResponseEntry(entry, fhirContext);
    return (Parameters) entry.getResource();
  }

  /**
   * Ensures that the bundle-response entry contains a successful response or otherwise throws the
   * appropriate subclass of {@link BaseServerResponseException}  capturing the error information
   * returned from the terminology server.
   *
   * <p>
   * This is adapted from HAPI: `ca.uhn.fhir.rest.client.impl.BaseClient#invokeClient(...)`.
   *
   * @param entry the bundle entry.
   * @param fhirContext the Fhir context to use.
   * @throws BaseServerResponseException then the entry status code is not 2xx.
   */
  private static void checkResponseEntry(@Nonnull final BundleEntryComponent entry, @Nonnull final
  FhirContext fhirContext) {

    final BundleEntryResponseComponent response = entry.getResponse();
    final int statusCode = Integer.parseInt(response.getStatus());

    if (statusCode < 200 || statusCode > 299) {

      String message = "Error in response entry : HTTP " + response.getStatus();
      final IBaseOperationOutcome oo = (IBaseOperationOutcome) entry.getResource();

      final String details = OperationOutcomeUtil.getFirstIssueDetails(fhirContext, oo);
      if (StringUtils.isNotBlank(details)) {
        message = message + " : " + details;
      }
      final BaseServerResponseException exception = BaseServerResponseException
          .newInstance(statusCode, message);
      exception.setOperationOutcome(oo);
      throw exception;
    }

  }


}
