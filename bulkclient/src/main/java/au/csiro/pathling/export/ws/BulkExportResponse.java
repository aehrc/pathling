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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BulkExportResponse implements  AsyncResponse {

  @Nonnull
  Long transactionTime;

  @Nonnull
  String request;

  boolean requiresAccessToken;

  @Nonnull
  @Builder.Default
  List<ResourceElement> output = Collections.emptyList();

  @Nonnull
  @Builder.Default
  List<ResourceElement> deleted = Collections.emptyList();

  @Nonnull
  @Builder.Default
  List<ResourceElement> error = Collections.emptyList();

  @Value
  public static class ResourceElement {

    @Nonnull
    String type;
    @Nonnull
    String url;

    long count;
  }
}
