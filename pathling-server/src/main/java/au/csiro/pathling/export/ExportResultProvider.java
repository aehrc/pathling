package au.csiro.pathling.export;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static java.util.Objects.requireNonNull;

/**
 * @author Felix Naumann
 */
@Component
@Profile("server")
@Slf4j
public class ExportResultProvider {

  private static final Pattern ID_PATTERN = Pattern.compile("^\\w{1,50}$");
  private final RequestTagFactory requestTagFactory;
  private final JobRegistry jobRegistry;
  private final ExportResultRegistry exportResultRegistry;
  private final String databasePath;

  @Autowired
  public ExportResultProvider(RequestTagFactory requestTagFactory, JobRegistry jobRegistry,
      ExportResultRegistry exportResultRegistry, @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") String databasePath) {
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.exportResultRegistry = exportResultRegistry;
    this.databasePath = databasePath;
  }

  /**
   * Enables the download of the result of an export operation.
   *
   * @param jobId the job jobId of the export request
   * @param response the {@link HttpServletResponse} for updating the response
   * processing
   */
  @SuppressWarnings({"unused", "TypeMayBeWeakened"})
  @OperationAccess("export")
  @Operation(name = "$result", idempotent = true, manualResponse = true)
  public void result(
      @Nonnull @OperationParam(name = "job") final String jobId,
      @Nonnull @OperationParam(name = "file") final String file,
      @Nullable final HttpServletResponse response) {
    requireNonNull(response);

    // Validate that the ID looks reasonable.
    try {
      UUID.fromString(jobId);
    } catch (IllegalArgumentException e) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    log.info("Retrieving export result: {}", jobId);
    
    ExportResult exportResult = exportResultRegistry.get(jobId);
    if(exportResult == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      throw new ResourceNotFoundError("Unknown job id.");
    }
    Optional<String> ownerId = exportResultRegistry.get(jobId).ownerId();
    
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownerId.equals(currentUserId)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'.".formatted(
              currentUserId.orElse("null")));
    }
    Path requestedFilepath = new Path(URI.create(databasePath).getPath() + Path.SEPARATOR + "jobs" + Path.SEPARATOR + jobId + Path.SEPARATOR + file);
    Resource resource = new FileSystemResource(requestedFilepath.toString());

    if(!resource.exists() || !resource.isFile()) {
      throw new ResourceNotFoundError("File '%s' does not exist or is not a file!".formatted(requestedFilepath.toString()));
    }
    
    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file + "\"");
    response.setContentType("application/octet-stream");
    response.setStatus(HttpServletResponse.SC_OK);
    try (InputStream inputStream = new FileInputStream(resource.getFile());
        OutputStream outputStream = response.getOutputStream()) {

      inputStream.transferTo(outputStream);
      outputStream.flush();

    } catch (IOException e) {
      throw new InternalErrorException("Failed transferring file!", e);
    }
  }
}
