/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.utilities;

import static org.apache.hadoop.shaded.org.apache.commons.io.IOUtils.closeQuietly;

import jakarta.annotation.Nonnull;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Closeable} that closes a collection of underlying resources.
 *
 * @author Piotr Szul
 */
@Slf4j
public class ResourceCloser implements Closeable {

  @Nonnull private final List<Closeable> resourcesToClose;

  /**
   * Constructs a new {@link ResourceCloser} with provided resources to close.
   *
   * @param resourcesToClose the resources to close
   */
  protected ResourceCloser(@Nonnull final Closeable... resourcesToClose) {
    this.resourcesToClose = new ArrayList<>(Arrays.asList(resourcesToClose));
  }

  /**
   * Adds a resource to the list of resources to close.
   *
   * @param resource the resource to close
   * @param <T> the type of the resource
   * @return the resource
   */
  protected <T extends Closeable> T registerResource(@Nonnull final T resource) {
    synchronized (resourcesToClose) {
      resourcesToClose.add(resource);
      return resource;
    }
  }

  @Override
  public void close() {
    log.debug("Closing {} resources for: {}", resourcesToClose.size(), this);
    synchronized (resourcesToClose) {
      for (final Closeable closeable : resourcesToClose) {
        log.debug("Closing resource: {} in: {}", closeable, this);
        closeQuietly(
            closeable,
            ex -> log.warn("Ignoring an error while closing resource: {}", closeable, ex));
      }
    }
  }
}
