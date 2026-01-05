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

package au.csiro.pathling.interceptors;

import au.csiro.pathling.async.JobProvider;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Interceptor for handling DELETE requests to the $job endpoint.
 *
 * @author Felix Naumann
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
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
    if (!request.getMethod().equals("DELETE") || !request.getPathInfo().matches(".*/\\$job$")) {
      return true;
    }

    jobProvider.deleteJob(request.getParameter("id"));

    return false;
  }
}
