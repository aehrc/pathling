package au.csiro.pathling.export.ws;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;

/**
 * Callback interface for handling asynchronous responses.
 *
 * @param <R> the type of the response
 * @param <T> the type of the result
 */
@FunctionalInterface
public interface AsyncResponseCallback<R extends AsyncResponse, T> {

  /**
   * Callback method to be called when the response is available.
   *
   * @param response the response
   * @param timeoutAfter the time allowed to handle the response
   * @return the result of handling the response
   * @throws IOException if an error occurs while handling the response
   */
  @Nonnull
  T handleResponse(@Nonnull final R response, @Nonnull final Duration timeoutAfter)
      throws IOException;

}
