/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static java.lang.Runtime.getRuntime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.api.model.Ports.Binding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.okhttp.OkHttpDockerCmdExecFactory;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * This test runs up the Docker container, imports data and interacts with the FHIR API.
 *
 * @author John Grimes
 */
@Tag("SystemTest")
@Slf4j
public class DockerImageTest {

  private static final String FHIR_SERVER_CONTAINER_NAME = "pathling-test-fhir-server";
  private static final String FHIR_SERVER_STAGING_PATH = "/usr/share/staging/test";

  // These system properties need to be set.
  private static final String VERSION = System.getProperty("version");
  private static final String TERMINOLOGY_SERVICE_URL = System.getProperty("terminologyServiceUrl");
  private static final String DOCKER_REPOSITORY = System.getProperty("dockerRepository");

  @Nonnull
  private final DockerClient dockerClient;

  @Nonnull
  private final HttpClient httpClient;

  @Nonnull
  private final IParser jsonParser;

  @Nullable
  private String fhirServerContainerId;

  @Nullable
  private StopContainer shutdownHook;

  public DockerImageTest() {
    final DockerClientConfig dockerClientConfig = DefaultDockerClientConfig
        .createDefaultConfigBuilder()
        .build();
    dockerClient = DockerClientBuilder.getInstance(dockerClientConfig)
        .withDockerCmdExecFactory(new OkHttpDockerCmdExecFactory())
        .build();
    httpClient = HttpClients.createDefault();
    jsonParser = FhirContext.forR4().newJsonParser();
    log.info("Created DockerImageTest: version=" + VERSION + ", terminologyServiceUrl="
        + TERMINOLOGY_SERVICE_URL + ", dockerRepository=" + DOCKER_REPOSITORY);
  }

  @Nonnull
  private static File[] getResourceFolderFiles(
      @SuppressWarnings("SameParameterValue") @Nonnull final String folder) {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    final URL url = loader.getResource(folder);
    assertThat(url).isNotNull();
    final String path = url.getPath();
    @Nullable final File[] files = new File(path).listFiles();
    assertNotNull(files);
    return files;
  }

  private static void stopContainer(@Nonnull final DockerClient dockerClient,
      @Nullable final String containerId) {
    if (containerId != null) {
      log.info("Stopping container");
      dockerClient.stopContainerCmd(containerId).exec();
      dockerClient.removeContainerCmd(containerId).exec();
      log.info("Container stopped and removed");
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    try {
      // Create the FHIR server container.
      final ExposedPort fhirServerPort = ExposedPort.tcp(8080);
      final PortBinding fhirServerPortBinding = new PortBinding(Binding.bindPort(8091),
          fhirServerPort);
      final HostConfig fhirServerHostConfig = new HostConfig();
      fhirServerHostConfig.withPortBindings(fhirServerPortBinding);
      final CreateContainerResponse fhirServerContainer = dockerClient
          .createContainerCmd(DOCKER_REPOSITORY + ":" + VERSION)
          .withExposedPorts(fhirServerPort)
          .withHostConfig(fhirServerHostConfig)
          .withEnv(
              "PATHLING_TERMINOLOGY_SERVER_URL=" + TERMINOLOGY_SERVICE_URL)
          .withName(FHIR_SERVER_CONTAINER_NAME)
          .exec();

      // Start the container.
      dockerClient.startContainerCmd(fhirServerContainer.getId()).exec();
      fhirServerContainerId = fhirServerContainer.getId();
      // Add a shutdown hook, so that we always clean up after ourselves even if the test process
      // is terminated abnormally.
      shutdownHook = new StopContainer(dockerClient, fhirServerContainerId);
      getRuntime().addShutdownHook(shutdownHook);
      log.info("FHIR server container started");

      // Wait until the container reaches a healthy state.
      boolean healthy = false;
      log.info("Waiting for container to achieve healthy state");
      while (!healthy) {
        final List<Container> containers = dockerClient.listContainersCmd()
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
      log.info("FHIR server container healthy");

      // Save the test resources into a staging area inside the container.
      log.info("Loading test data into container");
      for (final File testFile : getResourceFolderFiles("test-data/fhir")) {
        log.debug(
            "Copying " + testFile.getAbsolutePath() + " to " + FHIR_SERVER_STAGING_PATH
                + " within container");
        dockerClient.copyArchiveToContainerCmd("pathling-test-fhir-server")
            .withHostResource(testFile.getAbsolutePath())
            .withRemotePath(FHIR_SERVER_STAGING_PATH)
            .exec();
      }
      log.info("Test data load complete");
    } catch (final Exception e) {
      stopContainer(dockerClient, fhirServerContainerId);
      fhirServerContainerId = null;
      if (shutdownHook != null) {
        getRuntime().removeShutdownHook(shutdownHook);
      }
      throw e;
    }
  }

  @Test
  public void importDataAndQuery() throws JSONException, IOException {
    try {
      // Create a request to the $import operation, referencing the NDJSON files we have loaded into
      // the staging area.
      final InputStream requestStream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream("import/SystemTest-request.Parameters.json");
      assertThat(requestStream).isNotNull();

      final HttpPost importRequest = new HttpPost("http://localhost:8091/fhir/$import");
      importRequest.setEntity(new InputStreamEntity(requestStream));
      importRequest.addHeader("Content-Type", "application/json");
      importRequest.addHeader("Accept", "application/fhir+json");

      log.info("Sending import request");
      final OperationOutcome importOutcome;
      try (final CloseableHttpResponse response = (CloseableHttpResponse) httpClient
          .execute(importRequest)) {
        final InputStream importResponseStream = response.getEntity().getContent();
        importOutcome = (OperationOutcome) jsonParser
            .parseResource(importResponseStream);
        assertThat(response.getStatusLine().getStatusCode())
            .withFailMessage(importOutcome.getIssueFirstRep().getDiagnostics())
            .isEqualTo(200);
      }
      assertThat(importOutcome.getIssueFirstRep().getDiagnostics())
          .isEqualTo("Data import completed successfully");
      log.info("Import completed successfully");

      final Parameters inParams = new Parameters();

      // Set subject resource parameter.
      final ParametersParameterComponent subjectResourceParam = new ParametersParameterComponent();
      subjectResourceParam.setName("subjectResource");
      subjectResourceParam.setValue(new CodeType("Patient"));

      // Add aggregation, number of patients.
      final ParametersParameterComponent aggregationParam = new ParametersParameterComponent();
      aggregationParam.setName("aggregation");
      final ParametersParameterComponent aggregationExpression = new ParametersParameterComponent();
      aggregationExpression.setName("expression");
      aggregationExpression.setValue(new StringType("count()"));
      final ParametersParameterComponent aggregationLabel = new ParametersParameterComponent();
      aggregationLabel.setName("label");
      aggregationLabel.setValue(new StringType("Number of patients"));
      aggregationParam.getPart().add(aggregationExpression);
      aggregationParam.getPart().add(aggregationLabel);

      // Add grouping, has the patient been diagnosed with a chronic disease?
      final ParametersParameterComponent groupingParam = new ParametersParameterComponent();
      groupingParam.setName("grouping");
      final ParametersParameterComponent groupingExpression = new ParametersParameterComponent();
      groupingExpression.setName("expression");
      groupingExpression.setValue(new StringType(
          "reverseResolve(Condition.subject)"
              + ".code"
              + ".memberOf('http://snomed.info/sct?fhir_vs=ecl/"
              + "^ 32570581000036105|Problem/Diagnosis reference set| : "
              + "<< 263502005|Clinical course| = << 90734009|Chronic|')"));
      final ParametersParameterComponent groupingLabel = new ParametersParameterComponent();
      groupingLabel.setName("label");
      groupingLabel.setValue(new StringType("Diagnosed with chronic disease?"));
      groupingParam.getPart().add(groupingExpression);
      groupingParam.getPart().add(groupingLabel);

      // Add filter, females only.
      final ParametersParameterComponent filterParam = new ParametersParameterComponent();
      filterParam.setName("filter");
      filterParam.setValue(new StringType("gender = 'female'"));

      inParams.getParameter().add(subjectResourceParam);
      inParams.getParameter().add(aggregationParam);
      inParams.getParameter().add(groupingParam);
      inParams.getParameter().add(filterParam);

      // Send a request to the `$query` operation on the FHIR server.
      final String requestString = jsonParser.encodeResourceToString(inParams);
      final HttpPost queryRequest = new HttpPost("http://localhost:8091/fhir/$aggregate");
      queryRequest.setEntity(new StringEntity(requestString));
      queryRequest.addHeader("Content-Type", "application/fhir+json");
      queryRequest.addHeader("Accept", "application/fhir+json");

      log.info("Sending query request");
      try (final CloseableHttpResponse response = (CloseableHttpResponse) httpClient
          .execute(queryRequest)) {
        final int statusCode = response.getStatusLine().getStatusCode();
        final InputStream queryResponseStream = response.getEntity().getContent();
        if (statusCode == 200) {
          final StringWriter writer = new StringWriter();
          IOUtils.copy(queryResponseStream, writer, StandardCharsets.UTF_8);
          assertJson("responses/DockerImageTest-importDataAndQuery.Parameters.json",
              writer.toString()
          );
        } else {
          if (fhirServerContainerId != null) {
            final ResultCallback<Frame> callback = new LogCallback();
            dockerClient.logContainerCmd(fhirServerContainerId)
                .withStdOut(true)
                .withStdErr(true)
                .exec(callback);
          }
          final OperationOutcome opOutcome = (OperationOutcome) jsonParser
              .parseResource(queryResponseStream);
          assertEquals(200, statusCode, opOutcome.getIssueFirstRep().getDiagnostics());
        }
      }

    } finally {
      stopContainer(dockerClient, fhirServerContainerId);
      fhirServerContainerId = null;
      getRuntime().removeShutdownHook(shutdownHook);
    }
  }

  @AfterEach
  public void tearDown() {
    stopContainer(dockerClient, fhirServerContainerId);
    fhirServerContainerId = null;
    getRuntime().removeShutdownHook(shutdownHook);
  }

  static class StopContainer extends Thread {

    private final DockerClient dockerClient;
    private final String containerId;

    private StopContainer(final DockerClient dockerClient, final String containerId) {
      this.dockerClient = dockerClient;
      this.containerId = containerId;
    }

    public void run() {
      stopContainer(dockerClient, containerId);
    }

  }

  static class LogCallback implements ResultCallback<Frame> {

    @Override
    public void onStart(final Closeable closeable) {
      log.info("Commencing log capture");
    }

    @Override
    public void onNext(final Frame frame) {
      log.error(new String(frame.getPayload(), StandardCharsets.UTF_8));
    }

    @Override
    public void onError(final Throwable throwable) {
      log.error("Error retrieving logs", throwable);
    }

    @Override
    public void onComplete() {
      log.info("Log capture complete");
    }

    @Override
    public void close() {
    }

  }

}
