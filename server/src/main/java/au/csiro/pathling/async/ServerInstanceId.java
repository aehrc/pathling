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

package au.csiro.pathling.async;

import jakarta.annotation.Nonnull;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Provides a unique identifier for this server instance, generated at startup. This ID is used to
 * ensure that cached 202 Accepted responses from async operations are invalidated after a server
 * restart, preventing clients from polling stale job IDs that no longer exist in the in-memory job
 * registry.
 *
 * @author John Grimes
 */
@Component
public class ServerInstanceId {

  @Nonnull private final String id;

  /** Creates a new ServerInstanceId with a unique 8-character UUID prefix. */
  public ServerInstanceId() {
    this.id = UUID.randomUUID().toString().substring(0, 8);
  }

  /**
   * Returns the unique server instance identifier.
   *
   * @return the 8-character instance ID
   */
  @Nonnull
  public String getId() {
    return id;
  }
}
