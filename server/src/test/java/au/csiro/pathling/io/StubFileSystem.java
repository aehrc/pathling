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

package au.csiro.pathling.io;

import java.net.URI;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * A test-only Hadoop {@link org.apache.hadoop.fs.FileSystem} that behaves exactly like {@link
 * RawLocalFileSystem} (storing data on the local disk, keyed on the path portion) but reports the
 * {@code stub} scheme rather than {@code file}.
 *
 * <p>Registering this under {@code fs.stub.impl} and pointing a warehouse URI at {@code stub://...}
 * while {@code fs.defaultFS} remains {@code file:///} reproduces the exact default-versus-warehouse
 * scheme split that caused the production {@code Wrong FS} failure, as a fast, dependency-free unit
 * test. Code that resolves the filesystem from the warehouse URI works; code that resolves it from
 * the process default filesystem and then operates on a {@code stub://} path fails with {@code
 * Wrong FS}, just as it does against {@code s3a://} in production.
 *
 * @author John Grimes
 */
public class StubFileSystem extends RawLocalFileSystem {

  /** The scheme reported by this filesystem, distinct from the local {@code file} scheme. */
  static final String SCHEME = "stub";

  private static final URI STUB_URI = URI.create(SCHEME + ":///");

  @Override
  public URI getUri() {
    return STUB_URI;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }
}
