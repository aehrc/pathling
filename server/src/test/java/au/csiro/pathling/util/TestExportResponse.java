package au.csiro.pathling.util;

import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.operations.bulkexport.ExportExecutor;
import au.csiro.pathling.operations.bulkexport.ExportResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.hl7.fhir.r4.model.Parameters;

/**
 * This record is necessary to perform unit tests in the {@link ExportExecutor}. Otherwise, the unit
 * test invoking the execute method does not know the job UUID this belongs to.
 *
 * @param fakeJobId The fake job UUID made available to the testing method.
 * @param exportResponse The actual response from the execute method invocation.
 * @author Felix Naumann
 */
@VisibleForTesting
public record TestExportResponse(UUID fakeJobId, ExportResponse exportResponse) {

  public String getKickOffRequestUrl() {
    return exportResponse.getKickOffRequestUrl();
  }

  public WriteDetails getWriteDetails() {
    return exportResponse.getWriteDetails();
  }

  public Parameters toOutput() {
    return exportResponse.toOutput();
  }
}
