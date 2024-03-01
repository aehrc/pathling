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

package au.csiro.pathling.auth;

import org.apache.http.auth.AuthScope;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;

public class AuthContext {

  public interface TokenProvider extends AutoCloseable {

    /**
     * Get the current token.
     *
     * @return the current token, if available
     */
    @Nonnull
    Optional<String> getToken(@Nonnull final AuthScope requestScope);


    default <T> T withToken(@Nonnull final Callable<T> supplier) {
      return AuthContext.withProvider(this, supplier);
    }

    @Override
    default void close() throws IOException {
      //noop
    }
  }

  private static final ThreadLocal<TokenProvider> CONTEXT = new ThreadLocal<>();
  private static final TokenProvider DEFAULT_PROVIDER = new TokenProvider() {
    @Nonnull
    @Override
    public Optional<String> getToken(@Nonnull final AuthScope requestScope) {
      return Optional.empty();
    }
  };

  public static Optional<String> getToken(@Nonnull final AuthScope scope) {
    return Optional.ofNullable(CONTEXT.get()).flatMap(tp -> tp.getToken(scope));
  }

  private static <T> T withProvider(@Nonnull final TokenProvider tokenProvider, @Nonnull final
  Callable<T> supplier) {
    try {
      CONTEXT.set(tokenProvider);
      return supplier.call();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      CONTEXT.remove();
    }
  }

  @Nonnull
  public static TokenProvider noAuthProvider() {
    return DEFAULT_PROVIDER;
  }
}
