/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import au.csiro.clinsight.fhir.FhirServer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * @author John Grimes
 */
public class FhirServerContainer {

  public static void main(String[] args) throws Exception {
    start();
  }

  private static void start() throws Exception {
    final int maxThreads = 100;
    final int minThreads = 10;
    final int idleTimeout = 120;

    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

    Server server = new Server(threadPool);
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(8090);
    server.setConnectors(new Connector[]{connector});

    ServletHandler servletHandler = new ServletHandler();
    servletHandler.addServletWithMapping(FhirServer.class, "/fhir/*");
    server.setHandler(servletHandler);

    server.start();
  }

}
