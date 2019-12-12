/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.utilities.Configuration.setStringPropsUsingEnvVar;

import au.csiro.clinsight.fhir.AnalyticsServer;
import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.util.Arrays;
import java.util.HashMap;
import javax.annotation.Nonnull;
import org.eclipse.jetty.server.*;
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
    AnalyticsServerConfiguration config = new AnalyticsServerConfiguration();

    String httpPortString = System.getenv("CLINSIGHT_HTTP_PORT");
    int httpPort = 8080;
    if (httpPortString != null) {
      httpPort = Integer.parseInt(httpPortString);
    }
    setStringPropsUsingEnvVar(config, new HashMap<String, String>() {{
      put("CLINSIGHT_FHIR_SERVER_VERSION", "version");
      put("CLINSIGHT_WAREHOUSE_URL", "warehouseUrl");
      put("CLINSIGHT_SPARK_MASTER_URL", "sparkMasterUrl");
      put("CLINSIGHT_DATABASE_NAME", "databaseName");
      put("CLINSIGHT_EXECUTOR_MEMORY", "executorMemory");
      put("CLINSIGHT_TERMINOLOGY_SERVER_URL", "terminologyServerUrl");
      put("CLINSIGHT_AWS_ACCESS_KEY_ID", "awsAccessKeyId");
      put("CLINSIGHT_AWS_SECRET_ACCESS_KEY", "awsSecretAccessKey");
    }});
    String explainQueries = System.getenv("CLINSIGHT_EXPLAIN_QUERIES");
    config.setExplainQueries(explainQueries != null && explainQueries.equals("true"));
    String shufflePartitions = System.getenv("CLINSIGHT_SHUFFLE_PARTITIONS");
    if (shufflePartitions != null) {
      config.setShufflePartitions(Integer.parseInt(shufflePartitions));
    }
    String corsAllowedOrigins = System.getenv("CLINSIGHT_CORS_ALLOWED_ORIGINS");
    if (corsAllowedOrigins != null) {
      config.setCorsAllowedOrigins(Arrays.asList(corsAllowedOrigins.split(",")));
    }

    // This is required to force the use of the Woodstox StAX implementation. If you don't use
    // Woodstox, parsing falls over when reading in resources (e.g. StructureDefinitions) that
    // contain HTML entities.
    //
    // See: http://hapifhir.io/download.html#_toc_stax__woodstox
    System.setProperty("javax.xml.stream.XMLInputFactory", "com.ctc.wstx.stax.WstxInputFactory");
    System.setProperty("javax.xml.stream.XMLOutputFactory", "com.ctc.wstx.stax.WstxOutputFactory");
    System.setProperty("javax.xml.stream.XMLEventFactory", "com.ctc.wstx.stax.WstxEventFactory");

    start(httpPort, config);
  }

  private static void start(int httpPort, @Nonnull AnalyticsServerConfiguration configuration)
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
    servletHandler.addServletWithMapping(analyticsServletHolder, "/fhir/*");

    server.setHandler(servletHandler);

    server.start();
  }

}
