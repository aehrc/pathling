/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.SparkException;

/**
 * @author John Grimes
 */
public class ErrorHandling {

  /**
   * @param error the source {@link Throwable} object
   * @return a subclass of {@link BaseServerResponseException} that is appropriate to communicate
   * the error back to the user
   */
  public static BaseServerResponseException handleError(@Nonnull final Throwable error) {
    try {
      throw error;

    } catch (final SparkException | UncheckedExecutionException e) {
      // A SparkException is thrown when an error occurs inside a Spark job. In this case we unwrap
      // its cause and pass it back to this same method to be re-evaluated.
      @Nullable final Throwable cause = e.getCause();
      if (cause == null) {
        return new InternalErrorException("Unexpected error occurred", e);
      } else {
        return handleError(cause);
      }
    } catch (final BaseServerResponseException e) {
      // We pass HAPI exceptions through unaltered. These could come from the HAPI client when
      // querying the terminology service.
      return e;

    } catch (final ResourceNotFoundError e) {
      // Errors relating to resources not found are passed through using the corresponding HAPI
      // exception.
      return new ResourceNotFoundException(e.getMessage());

    } catch (final InvalidUserInputError e) {
      // Errors relating to invalid user input are passed through using the corresponding HAPI
      // exception.
      return new InvalidRequestException(e);

    } catch (final Throwable e) {
      // Anything else is unexpected and triggers a 500.
      return new InternalErrorException("Unexpected error occurred", e);
    }
  }

}
