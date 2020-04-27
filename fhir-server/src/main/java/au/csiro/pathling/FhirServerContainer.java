/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.utilities.Configuration.setStringPropsUsingEnvVar;

import au.csiro.pathling.fhir.AnalyticsServer;
import au.csiro.pathling.fhir.AnalyticsServerConfiguration;
import io.sentry.Sentry;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Creates an embedded Jetty server and mounts the servlets that are required to deliver the
 * functionality of the FHIR analytics server. Also knows how to configure the servlets using a set
 * of environment variables.
 *
 * @author John Grimes
 */
public class FhirServerContainer {

  public static void main(String[] args) throws Exception {
    // Initialise Sentry, for reporting errors when the `SENTRY_DSN` environment variable is set.
    Sentry.init();

    AnalyticsServerConfiguration config = new AnalyticsServerConfiguration();

    String httpPortString = System.getenv("PATHLING_HTTP_PORT");
    int httpPort = 8080;
    if (httpPortString != null) {
      httpPort = Integer.parseInt(httpPortString);
    }
    String httpBase = System.getenv("PATHLING_HTTP_BASE");
    if (httpBase == null) {
      httpBase = "";
    }
    setStringPropsUsingEnvVar(config, new HashMap<String, String>() {{
      put("PATHLING_FHIR_SERVER_VERSION", "version");
      put("PATHLING_WAREHOUSE_URL", "warehouseUrl");
      put("PATHLING_SPARK_MASTER_URL", "sparkMasterUrl");
      put("PATHLING_DATABASE_NAME", "databaseName");
      put("PATHLING_EXECUTOR_MEMORY", "executorMemory");
      put("PATHLING_TERMINOLOGY_SERVER_URL", "terminologyServerUrl");
      put("PATHLING_AWS_ACCESS_KEY_ID", "awsAccessKeyId");
      put("PATHLING_AWS_SECRET_ACCESS_KEY", "awsSecretAccessKey");
      put("PATHLING_AUTH_JWKS_URL", "authJwksUrl");
      put("PATHLING_AUTH_ISSUER", "authIssuer");
      put("PATHLING_AUTH_AUDIENCE", "authAudience");
      put("PATHLING_AUTH_AUTHORIZE_URL", "authorizeUrl");
      put("PATHLING_AUTH_TOKEN_URL", "tokenUrl");
      put("PATHLING_AUTH_REVOKE_URL", "revokeTokenUrl");
    }});
    String explainQueries = System.getenv("PATHLING_EXPLAIN_QUERIES");
    config.setExplainQueries(explainQueries != null && explainQueries.equals("true"));
    String authEnabled = System.getenv("PATHLING_AUTH_ENABLED");
    config.setAuthEnabled(authEnabled != null && authEnabled.equals("true"));
    String verboseRequestLogging = System.getenv("PATHLING_VERBOSE_REQUEST_LOGGING");
    config.setVerboseRequestLogging(
        verboseRequestLogging != null && verboseRequestLogging.equals("true"));
    String shufflePartitions = System.getenv("PATHLING_SHUFFLE_PARTITIONS");
    if (shufflePartitions != null) {
      config.setShufflePartitions(Integer.parseInt(shufflePartitions));
    }
    String corsAllowedOrigins = System.getenv("PATHLING_CORS_ALLOWED_ORIGINS");
    if (corsAllowedOrigins != null) {
      config.setCorsAllowedOrigins(Arrays.asList(corsAllowedOrigins.split(",")));
    }
    String terminologySocketTimeoutString = System.getenv("PATHLING_TERMINOLOGY_SOCKET_TIMEOUT");
    if (terminologySocketTimeoutString != null) {
      config.setTerminologySocketTimeout(Integer.parseInt(terminologySocketTimeoutString));
    }

    // This is required to force the use of the Woodstox StAX implementation. If you don't use
    // Woodstox, parsing falls over when reading in resources (e.g. StructureDefinitions) that
    // contain HTML entities.
    //
    // See: http://hapifhir.io/download.html#_toc_stax__woodstox
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory");
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.ctc.wstx.stax.WstxOutputFactory");
    System.setProperty("javax.xml.stream.XMLEventFactory", "com.ctc.wstx.stax.WstxEventFactory");

    start(httpPort, httpBase, config);
  }

  private static void start(int httpPort, final String httpBase,
      @Nonnull AnalyticsServerConfiguration configuration)
      throws Exception {
    final int maxThreads = 100;
    final int minThreads = 10;
    final int idleTimeout = 120;

    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

    HttpConfiguration httpConfiguration = new HttpConfiguration();
    httpConfiguration.setSendServerVersion(false);
    HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfiguration);

    Server server = new Server(threadPool);
    ServerConnector connector = new ServerConnector(server, httpConnectionFactory);
    connector.setPort(httpPort);
    server.setConnectors(new Connector[]{connector});

    ServletHandler servletHandler = new ServletHandler();

    // Add analytics server to handle all other requests.
    ServletHolder analyticsServletHolder = new ServletHolder(new AnalyticsServer(configuration));
    servletHandler.addServletWithMapping(analyticsServletHolder, httpBase + "/fhir/*");

    server.setHandler(servletHandler);
    server.setErrorHandler(new TerseErrorHandler());

    server.start();
  }

  static class TerseErrorHandler extends ErrorHandler {

    @Override
    protected void writeErrorPageBody(HttpServletRequest request, Writer writer, int code,
        String message, boolean showStacks) throws IOException {
      writer.write("<p>" + message + "</p>");
    }

  }

}
