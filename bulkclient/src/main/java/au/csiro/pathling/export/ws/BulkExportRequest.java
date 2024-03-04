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

@Value
@Builder()
public class BulkExportRequest {

  public interface Operation {

    @Nonnull
    String getPath();

    boolean isPatientSupported();
  }

  @Value
  public static class SystemLevel implements Operation {

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

  @Value
  public static class PatientLevel implements Operation {


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

  @Value
  public static class GroupLevel implements Operation {

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

  @Nonnull
  @Builder.Default
  Operation operation = new SystemLevel();

  @Nonnull
  @Builder.Default
  String _outputFormat = "ndjson";

  @Nonnull
  @Builder.Default
  List<String> _type = Collections.emptyList();


  @Nonnull
  @Builder.Default
  List<String> _elements = Collections.emptyList();

  @Nonnull
  @Builder.Default
  List<String> _typeFilter = Collections.emptyList();

  @Nullable
  @Builder.Default
  Instant _since = null;

  @Nonnull
  @Builder.Default
  List<Reference> patient = Collections.emptyList();

  @Nonnull
  public Parameters toParameters() {

    final List<Parameter> params = Stream.of(
            Stream.of(
                Parameter.of("_outputFormat", _outputFormat)
            ),
            Optional.ofNullable(_since)
                .map(s -> Parameter.of("_since", s)).stream(),
            _type.stream()
                .map(t -> Parameter.of("_type", t)),
            Lists.optionalOf(_elements)
                .map(e -> Parameter.of("_elements", String.join(",", e))).stream(),
            Lists.optionalOf(_typeFilter)
                .map(f -> Parameter.of("_typeFilter", String.join(",", f))).stream(),
            patient.stream()
                .map(p -> Parameter.of("patient", p))
        )
        .flatMap(Function.identity())
        .collect(Collectors.toUnmodifiableList());

    return Parameters.builder()
        .parameter(params)
        .build();
  }
}
