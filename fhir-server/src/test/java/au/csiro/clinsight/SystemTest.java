/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test runs up the Docker container, uses a real HDFS cluster and a real instance of
 * Ontoserver.
 *
 * @author John Grimes
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "OptionalGetWithoutIsPresent"})
public class SystemTest {

  private static final Logger logger = LoggerFactory.getLogger(SystemTest.class);
  private static final String FHIR_SERVER_CONTAINER_NAME = "clinsight-test-fhir-server";
  private static final String FHIR_SERVER_STAGING_PATH = "/usr/share/staging/test";

  // These two system properties need to be set.
  private static final String FHIR_SERVER_DOCKER_TAG = System
      .getProperty("clinsight.test.fhirServerDockerTag");
  private static final String TERMINOLOGY_SERVICE_URL = System
      .getProperty("clinsight.test.terminologyServiceUrl");

  private final DockerClient dockerClient;
  private final HttpClient httpClient;
  private final IParser jsonParser;
  private String fhirServerContainerId;

  public SystemTest() {
    DockerClientConfig dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder()
        .build();
    dockerClient = DockerClientBuilder.getInstance(dockerClientConfig).build();
    httpClient = HttpClients.createDefault();
    jsonParser = FhirContext.forR4().newJsonParser();
  }

  private static File[] getResourceFolderFiles(String folder) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource(folder);
    assertThat(url).isNotNull();
    String path = url.getPath();
    return new File(path).listFiles();
  }

  private static void stopContainer(DockerClient dockerClient, String containerId) {
    if (containerId != null) {
      logger.info("Stopping container");
      dockerClient.stopContainerCmd(containerId).exec();
      dockerClient.removeContainerCmd(containerId).exec();
      logger.info("Container stopped and removed");
    }
  }

  @Before
  public void setUp() throws Exception {
    try {
      // Create the FHIR server container.
      ExposedPort fhirServerPort = ExposedPort.tcp(8080);
      PortBinding fhirServerPortBinding = new PortBinding(Binding.bindPort(8091), fhirServerPort);
      HostConfig fhirServerHostConfig = new HostConfig();
      fhirServerHostConfig.withPortBindings(fhirServerPortBinding);
      CreateContainerResponse fhirServerContainer = dockerClient
          .createContainerCmd(
              "docker-registry.it.csiro.au/clinsight/fhir-server:" + FHIR_SERVER_DOCKER_TAG)
          .withExposedPorts(fhirServerPort)
          .withHostConfig(fhirServerHostConfig)
          .withEnv(
              "CLINSIGHT_TERMINOLOGY_SERVER_URL=" + TERMINOLOGY_SERVICE_URL)
          .withName(FHIR_SERVER_CONTAINER_NAME)
          .exec();

      // Start the container.
      dockerClient.startContainerCmd(fhirServerContainer.getId()).exec();
      fhirServerContainerId = fhirServerContainer.getId();
      // Add a shutdown hook, so that we always clean up after ourselves even if the test process
      // is terminated abnormally.
      Runtime.getRuntime().addShutdownHook(new StopContainer(dockerClient, fhirServerContainerId));
      logger.info("FHIR server container started");

      // Wait until the container reaches a healthy state.
      boolean healthy = false;
      logger.info("Waiting for container to achieve healthy state");
      while (!healthy) {
        List<Container> containers = dockerClient.listContainersCmd()
            .withFilter("id", Collections.singletonList(fhirServerContainerId))
            .withFilter("health", Collections.singletonList("healthy"))
            .exec();
        healthy = containers.stream()
            .map(Container::getId)
            .collect(Collectors.toList())
            .contains(fhirServerContainer.getId());
        if (!healthy) {
          Thread.sleep(1000);
        }
      }
      logger.info("FHIR server container healthy");

      // Save the test resources into a staging area inside the container.
      logger.info("Loading test data into container");
      for (File testFile : getResourceFolderFiles("test-data/fhir")) {
        logger.debug(
            "Copying " + testFile.getAbsolutePath() + " to " + FHIR_SERVER_STAGING_PATH
                + " within container");
        dockerClient.copyArchiveToContainerCmd("clinsight-test-fhir-server")
            .withHostResource(testFile.getAbsolutePath())
            .withRemotePath(FHIR_SERVER_STAGING_PATH)
            .exec();
      }
      logger.info("Test data load complete");
    } catch (Exception e) {
      stopContainer(dockerClient, fhirServerContainerId);
      throw e;
    }
  }

  @Test
  public void importDataAndQuery() throws IOException {
    try {
      // Create a request to the $import operation, referencing the NDJSON files we have loaded into
      // the staging area.
      InputStream requestStream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("import/SystemTest-request.Parameters.json");
      assertThat(requestStream).isNotNull();

      HttpPost importRequest = new HttpPost("http://localhost:8091/fhir/$import");
      importRequest.setEntity(new InputStreamEntity(requestStream));
      importRequest.addHeader("Content-Type", "application/json");
      importRequest.addHeader("Accept", "application/fhir+json");

      logger.info("Sending import request");
      OperationOutcome importOutcome;
      try (CloseableHttpResponse response = (CloseableHttpResponse) httpClient
          .execute(importRequest)) {
        InputStream importResponseStream = response.getEntity().getContent();
        importOutcome = (OperationOutcome) jsonParser
            .parseResource(importResponseStream);
        assertThat(response.getStatusLine().getStatusCode())
            .withFailMessage(importOutcome.getIssueFirstRep().getDiagnostics())
            .isEqualTo(200);
      }
      assertThat(importOutcome.getIssueFirstRep().getDiagnostics())
          .isEqualTo("Data import completed successfully");
      logger.info("Import completed successfully");

      Parameters inParams = new Parameters();

      // Set subject resource parameter.
      ParametersParameterComponent subjectResourceParam = new ParametersParameterComponent();
      subjectResourceParam.setName("subjectResource");
      subjectResourceParam.setValue(new CodeType("Patient"));

      // Add aggregation, number of patients.
      ParametersParameterComponent aggregationParam = new ParametersParameterComponent();
      aggregationParam.setName("aggregation");
      ParametersParameterComponent aggregationExpression = new ParametersParameterComponent();
      aggregationExpression.setName("expression");
      aggregationExpression.setValue(new StringType("count"));
      ParametersParameterComponent aggregationLabel = new ParametersParameterComponent();
      aggregationLabel.setName("label");
      aggregationLabel.setValue(new StringType("Number of patients"));
      aggregationParam.getPart().add(aggregationExpression);
      aggregationParam.getPart().add(aggregationLabel);

      // Add grouping, has the patient been diagnosed with a chronic disease?
      ParametersParameterComponent groupingParam = new ParametersParameterComponent();
      groupingParam.setName("grouping");
      ParametersParameterComponent groupingExpression = new ParametersParameterComponent();
      groupingExpression.setName("expression");
      groupingExpression.setValue(new StringType(
          "reverseResolve(Condition.subject)"
              + ".code"
              + ".memberOf('http://snomed.info/sct?fhir_vs=ecl/"
              + "^ 32570581000036105|Problem/Diagnosis reference set| : "
              + "<< 263502005|Clinical course| = << 90734009|Chronic|')"));
      ParametersParameterComponent groupingLabel = new ParametersParameterComponent();
      groupingLabel.setName("label");
      groupingLabel.setValue(new StringType("Diagnosed with chronic disease?"));
      groupingParam.getPart().add(groupingExpression);
      groupingParam.getPart().add(groupingLabel);

      // Add filter, females only.
      ParametersParameterComponent filterParam = new ParametersParameterComponent();
      filterParam.setName("filter");
      filterParam.setValue(new StringType("gender = 'female'"));

      inParams.getParameter().add(subjectResourceParam);
      inParams.getParameter().add(aggregationParam);
      inParams.getParameter().add(groupingParam);
      inParams.getParameter().add(filterParam);

      // Send a request to the `$query` operation on the FHIR server.
      String requestString = jsonParser.encodeResourceToString(inParams);
      HttpPost queryRequest = new HttpPost("http://localhost:8091/fhir/$query");
      queryRequest.setEntity(new StringEntity(requestString));
      queryRequest.addHeader("Content-Type", "application/fhir+json");
      queryRequest.addHeader("Accept", "application/fhir+json");

      Parameters outParams = null;
      logger.info("Sending query request");
      try (CloseableHttpResponse response = (CloseableHttpResponse) httpClient
          .execute(queryRequest)) {
        int statusCode = response.getStatusLine().getStatusCode();
        InputStream queryResponseStream = response.getEntity().getContent();
        if (statusCode == 200) {
          outParams = (Parameters) jsonParser.parseResource(queryResponseStream);
        } else {
          OperationOutcome opOutcome = (OperationOutcome) jsonParser
              .parseResource(queryResponseStream);
          assertThat(statusCode)
              .withFailMessage(opOutcome.getIssueFirstRep().getDiagnostics())
              .isEqualTo(200);
        }
      }

      // Check the first grouping.
      ParametersParameterComponent firstGrouping = outParams.getParameter().get(0);
      Optional<ParametersParameterComponent> firstLabel = firstGrouping.getPart().stream()
          .filter(part -> part.getName().equals("label")).findFirst();
      assertThat(firstLabel.isPresent());
      assertThat(firstLabel.get().getValue().equals(new BooleanType(true)));
      Optional<ParametersParameterComponent> firstResult = firstGrouping.getPart().stream()
          .filter(part -> part.getName().equals("result")).findFirst();
      assertThat(firstResult.isPresent());
      assertThat(firstResult.get().getValue().equals(new UnsignedIntType(2)));

      // Check the second grouping.
      ParametersParameterComponent secondGrouping = outParams.getParameter().get(0);
      Optional<ParametersParameterComponent> secondLabel = secondGrouping.getPart().stream()
          .filter(part -> part.getName().equals("label")).findFirst();
      assertThat(secondLabel.isPresent());
      assertThat(secondLabel.get().getValue().equals(new BooleanType(true)));
      Optional<ParametersParameterComponent> secondResult = secondGrouping.getPart().stream()
          .filter(part -> part.getName().equals("result")).findFirst();
      assertThat(secondResult.isPresent());
      assertThat(secondResult.get().getValue().equals(new UnsignedIntType(2)));
    } finally {
      stopContainer(dockerClient, fhirServerContainerId);
    }
  }

  @After
  public void tearDown() {
    stopContainer(dockerClient, fhirServerContainerId);
  }

  static class StopContainer extends Thread {

    private final DockerClient dockerClient;
    private final String containerId;

    StopContainer(DockerClient dockerClient, String containerId) {
      this.dockerClient = dockerClient;
      this.containerId = containerId;
    }

    public void run() {
      stopContainer(dockerClient, containerId);
    }

  }

}
