package au.csiro.pathling.interceptors;

import au.csiro.pathling.async.JobProvider;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Component
public class BulkExportDeleteInterceptor {


  private final JobProvider jobProvider;

  public BulkExportDeleteInterceptor(JobProvider jobProvider) {
    this.jobProvider = jobProvider;
  }

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_POST_PROCESSED)
  public boolean interceptJobDeletion(
      HttpServletRequest request,
      HttpServletResponse response,
      ServletRequestDetails requestDetails) {
    if(!request.getMethod().equals("DELETE") || !request.getPathInfo().matches(".*/\\$job$")) {
      return true;
    }
    
    jobProvider.deleteJob(request.getParameter("id"));
    
    return false;
  }
}
