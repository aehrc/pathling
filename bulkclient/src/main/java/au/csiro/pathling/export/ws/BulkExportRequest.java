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

    String path = "$export";
    boolean patientSupported = false;
  }

  @Value
  public static class PatientLevel implements Operation {

    String path = "Patient/$export";
    boolean patientSupported = true;
  }

  @Value
  public static class GroupLevel implements Operation {

    boolean patientSupported = true;
    @Nonnull
    String id;

    @Nonnull
    @Override
    public String getPath() {
      return String.format("Group/%s/$export", id);
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
            patient.stream()
                .map(p -> Parameter.of("patient", p))
        )
        .flatMap(Function.identity())
        .collect(Collectors.toUnmodifiableList());

    return Parameters.builder()
        .parameter(params)
        .build();
  }

  @Nonnull
  public static BulkExportRequestBuilder systemBuilder() {
    return builder();
  }

  @Nonnull
  public static BulkExportRequestBuilder patientBuilder() {
    return new BulkExportRequestBuilder().operation(new PatientLevel());
  }

  @Nonnull
  public static BulkExportRequestBuilder groupBuilder(@Nonnull final String id) {
    return new BulkExportRequestBuilder().operation(new GroupLevel(id));
  }


}
