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

package au.csiro.pathling.interceptors;

import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.PrimitiveType;

/**
 * Interceptor that transforms FHIR Parameters responses to plain JSON when the client requests
 * application/json via the Accept header. This allows operations that return Parameters to provide
 * either FHIR-formatted or plain JSON responses based on client preference.
 *
 * <p>When Accept is application/json, Parameters are transformed using this algorithm:
 *
 * <ul>
 *   <li>Each parameter name becomes a JSON property
 *   <li>Primitive values are extracted to their JSON equivalents
 *   <li>Parameters with parts become nested JSON objects
 *   <li>Repeated parameter names become JSON arrays
 * </ul>
 *
 * @author John Grimes
 */
@Interceptor
@Slf4j
public class ParametersToJsonInterceptor {

  private static final String APPLICATION_JSON = "application/json";
  private static final String APPLICATION_FHIR_JSON = "application/fhir+json";
  private static final String NATIVE_JSON_KEY = "nativeJson";

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Hook that intercepts outgoing responses and transforms Parameters resources to plain JSON when
   * the client requests application/json.
   *
   * @param requestDetails the request details
   * @param resource the resource being returned
   * @param request the HTTP servlet request
   * @param response the HTTP servlet response
   * @return true to continue normal FHIR processing, false to skip it
   * @throws IOException if there is a problem writing to the response
   */
  @Hook(Pointcut.SERVER_OUTGOING_RESPONSE)
  @SuppressWarnings("unused")
  public boolean transformResponse(
      @Nullable final RequestDetails requestDetails,
      @Nullable final IBaseResource resource,
      @Nullable final HttpServletRequest request,
      @Nullable final HttpServletResponse response)
      throws IOException {

    // Only transform Parameters resources.
    if (!(resource instanceof final Parameters parameters)) {
      return true;
    }

    // Check if the client prefers plain JSON.
    if (request == null || !prefersPlainJson(request)) {
      return true;
    }

    if (response == null) {
      log.warn("Parameters to JSON interceptor invoked with missing servlet response");
      return true;
    }

    // Check for native JSON first (used by ExportResponse for lossless conversion).
    final Object nativeJson = parameters.getUserData(NATIVE_JSON_KEY);
    final String json = (nativeJson != null) ? (String) nativeJson : parametersToJson(parameters);

    response.setContentType(APPLICATION_JSON);
    response.setCharacterEncoding("UTF-8");
    response.getWriter().write(json);

    // Return false to skip normal FHIR serialisation.
    return false;
  }

  /**
   * Determines whether the client prefers plain JSON based on the Accept header. Returns true by
   * default, only returning false if the client explicitly requests FHIR JSON.
   *
   * @param request the HTTP servlet request
   * @return true if the client prefers plain JSON
   */
  private boolean prefersPlainJson(@Nonnull final HttpServletRequest request) {
    final String acceptHeader = request.getHeader("Accept");

    // Only use FHIR format if explicitly requested.
    if (acceptHeader != null && acceptHeader.contains(APPLICATION_FHIR_JSON)) {
      return false;
    }

    // Default to plain JSON for all other cases.
    return true;
  }

  /**
   * Transforms a FHIR Parameters resource into plain JSON.
   *
   * @param parameters the Parameters resource to transform
   * @return the JSON string representation
   * @throws IOException if there is a problem writing JSON
   */
  @Nonnull
  private String parametersToJson(@Nonnull final Parameters parameters) throws IOException {
    final ObjectNode json = mapper.createObjectNode();

    // Group parameters by name to handle repeated parameters as arrays.
    final Map<String, List<ParametersParameterComponent>> grouped = new LinkedHashMap<>();
    for (final ParametersParameterComponent param : parameters.getParameter()) {
      grouped.computeIfAbsent(param.getName(), k -> new java.util.ArrayList<>()).add(param);
    }

    // Process each group of parameters.
    for (final Map.Entry<String, List<ParametersParameterComponent>> entry : grouped.entrySet()) {
      final String name = entry.getKey();
      final List<ParametersParameterComponent> params = entry.getValue();

      if (params.size() == 1) {
        // Single parameter - add as direct value or object.
        addParameterValue(json, name, params.get(0));
      } else {
        // Multiple parameters with same name - create array.
        final ArrayNode array = json.putArray(name);
        for (final ParametersParameterComponent param : params) {
          addParameterToArray(array, param);
        }
      }
    }

    return mapper.writeValueAsString(json);
  }

  /**
   * Adds a parameter value to a JSON object.
   *
   * @param json the JSON object to add to
   * @param name the parameter name
   * @param param the parameter component
   */
  private void addParameterValue(
      @Nonnull final ObjectNode json,
      @Nonnull final String name,
      @Nonnull final ParametersParameterComponent param) {
    if (param.hasValue()) {
      // Primitive value.
      addPrimitiveValue(json, name, param.getValue());
    } else if (param.hasPart()) {
      // Nested parts - create object.
      json.set(name, partsToObject(param.getPart()));
    }
  }

  /**
   * Adds a parameter to a JSON array.
   *
   * @param array the JSON array to add to
   * @param param the parameter component
   */
  private void addParameterToArray(
      @Nonnull final ArrayNode array, @Nonnull final ParametersParameterComponent param) {
    if (param.hasValue()) {
      // Primitive value.
      addPrimitiveToArray(array, param.getValue());
    } else if (param.hasPart()) {
      // Nested parts - create object.
      array.add(partsToObject(param.getPart()));
    }
  }

  /**
   * Converts a list of parameter parts to a JSON object.
   *
   * @param parts the parameter parts
   * @return the JSON object
   */
  @Nonnull
  private ObjectNode partsToObject(@Nonnull final List<ParametersParameterComponent> parts) {
    final ObjectNode obj = mapper.createObjectNode();
    for (final ParametersParameterComponent part : parts) {
      addParameterValue(obj, part.getName(), part);
    }
    return obj;
  }

  /**
   * Adds a primitive FHIR value to a JSON object.
   *
   * @param json the JSON object to add to
   * @param name the property name
   * @param value the FHIR value
   */
  private void addPrimitiveValue(
      @Nonnull final ObjectNode json,
      @Nonnull final String name,
      @Nonnull final org.hl7.fhir.r4.model.Type value) {
    if (value instanceof final BooleanType booleanType) {
      json.put(name, booleanType.getValue());
    } else if (value instanceof final PrimitiveType<?> primitiveType) {
      json.put(name, primitiveType.getValueAsString());
    }
  }

  /**
   * Adds a primitive FHIR value to a JSON array.
   *
   * @param array the JSON array to add to
   * @param value the FHIR value
   */
  private void addPrimitiveToArray(
      @Nonnull final ArrayNode array, @Nonnull final org.hl7.fhir.r4.model.Type value) {
    if (value instanceof final BooleanType booleanType) {
      array.add(booleanType.getValue());
    } else if (value instanceof final PrimitiveType<?> primitiveType) {
      array.add(primitiveType.getValueAsString());
    }
  }
}
