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

package au.csiro.pathling.export.ws;

import au.csiro.pathling.export.fhir.Parameters;
import au.csiro.pathling.export.fhir.Parameters.Parameter;
import au.csiro.pathling.export.fhir.Reference;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.utilities.Lists;
import lombok.Builder;
import lombok.Value;

/**
 * Represents a request to initiate a bulk export operation.
 *
 * @see <a href="https://hl7.org/fhir/uv/bulkdata/export.html#query-parameters">FHIR Bulk Export
 * Request Query Parameters</a>
 */
@Value
@Builder()
public class BulkExportRequest {

  /**
   * The level of the export operation.
   */
  public interface Level {

    /**
     * The path to the export operation corresponding to this level.
     *
     * @return the path to the export operation.
     */
    @Nonnull
    String getPath();

    /**
     * Whether this level supports patient-specific exports.
     *
     * @return true if patient-specific exports are supported.
     */
    boolean isPatientSupported();
  }

  /**
   * Represents the system level of the export operation.
   */
  @Value
  public static class SystemLevel implements Level {

    @Nonnull
    @Override
    public String getPath() {
      return "$export";
    }

    @Override
    public boolean isPatientSupported() {
      return false;
    }
  }

  /**
   * Represents the patient level of the export operation.
   */
  @Value
  public static class PatientLevel implements Level {


    @Nonnull
    @Override
    public String getPath() {
      return "Patient/$export";
    }

    @Override
    public boolean isPatientSupported() {
      return true;
    }
  }

  /**
   * Represents the group level of the export operation.
   */
  @Value
  public static class GroupLevel implements Level {

    /**
     * The ID of the group.
     */
    @Nonnull
    String id;

    @Nonnull
    @Override
    public String getPath() {
      return String.format("Group/%s/$export", id);
    }

    @Override
    public boolean isPatientSupported() {
      return true;
    }
  }

  /**
   * The level of the export operation. The default is {@link SystemLevel}.
   */
  @Nonnull
  @Builder.Default
  Level level = new SystemLevel();

  /**
   * The format of the output. The value of the '_outputFormat' query parameter.
   */
  @Nullable
  @Builder.Default
  String _outputFormat = null;

  /**
   * The date and time to use as the lower bound for the export. The value of the '_since' query
   */
  @Nullable
  @Builder.Default
  Instant _since = null;

  /**
   * The types of resources to export. The value of the '_type' query parameter.
   */
  @Nonnull
  @Builder.Default
  List<String> _type = Collections.emptyList();


  /**
   * The elements to include in the export. The value of the '_elements' query parameter.
   */
  @Nonnull
  @Builder.Default
  List<String> _elements = Collections.emptyList();

  /**
   * The criteria to filter the resources to include in the export . The value of the '_typeFilter'
   * query parameter.
   */
  @Nonnull
  @Builder.Default
  List<String> _typeFilter = Collections.emptyList();


  /**
   * The patient(s) to include in the export. The value of the 'patient'  parameter.
   */
  @Nonnull
  @Builder.Default
  List<Reference> patient = Collections.emptyList();

  /**
   * Converts this request to a {@link Parameters} object.
   *
   * @return the parameters.
   */
  @Nonnull
  public Parameters toParameters() {

    final List<Parameter> params = Stream.of(
            Optional.ofNullable(_outputFormat)
                .map(s -> Parameter.of("_outputFormat", s)).stream(),
            Optional.ofNullable(_since)
                .map(s -> Parameter.of("_since", s)).stream(),
            Lists.optionalOf(_type)
                .map(e -> Parameter.of("_type", String.join(",", e))).stream(),
            Lists.optionalOf(_elements)
                .map(e -> Parameter.of("_elements", String.join(",", e))).stream(),
            Lists.optionalOf(_typeFilter)
                .map(f -> Parameter.of("_typeFilter", String.join(",", f))).stream(),
            patient.stream()
                .map(p -> Parameter.of("patient", p))
        )
        .flatMap(Function.identity())
        .collect(Collectors.toUnmodifiableList());
    return Parameters.of(params);
  }
}
