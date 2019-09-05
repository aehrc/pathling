/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * These tests run up the Docker container, use a real (local) instance of Apache Spark and a real
 * instance of Ontoserver.
 *
 * @author John Grimes
 */
public class SystemTest {

  private final List<String> createdNetworks = new ArrayList<>();
  private final List<String> createdContainers = new ArrayList<>();
  private final DockerClient dockerClient;

  public SystemTest() {
    DockerClientConfig dockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder()
        .build();
    dockerClient = DockerClientBuilder.getInstance(dockerClientConfig).build();
  }

  @Before
  public void setUp() throws Exception {
    // Create a network for the test containers.
    dockerClient.createNetworkCmd()
        .withDriver("bridge")
        .withName("clinsight-test")
        .exec();
    createdNetworks.add("clinsight-test");

    // Create the FHIR server container.
    ExposedPort exposedPort = ExposedPort.tcp(8090);
    PortBinding portBinding = new PortBinding(Binding.bindPort(8091), exposedPort);
    HostConfig hostConfig = new HostConfig();
    hostConfig.withPortBindings(portBinding);
    CreateContainerResponse fhirServerContainer = dockerClient
        .createContainerCmd("docker-registry.it.csiro.au/clinsight/fhir-server")
        .withExposedPorts(exposedPort)
        .withHostConfig(hostConfig)
        .withEnv(
            "CLINSIGHT_SPARK_MASTER_URL=local[4]",
            "CLINSIGHT_WAREHOUSE_DIRECTORY=/usr/share/warehouse",
            "CLINSIGHT_METASTORE_URL=jdbc:postgresql://clinsight-test-metastore/postgres",
            "CLINSIGHT_METASTORE_USER=postgres",
            "CLINSIGHT_DATABASE_NAME=postgres",
            "CLINSIGHT_TERMINOLOGY_SERVER_URL=http://clinsight-test-ontoserver:8080/fhir"
        ).withName("clinsight-test-fhir-server")
        .exec();
    createdContainers.add("clinsight-test-fhir-server");

    // Create the metastore container.
    CreateContainerResponse metastoreContainer = dockerClient
        .createContainerCmd("postgres")
        .withName("clinsight-test-metastore")
        .exec();
    createdContainers.add("clinsight-test-metastore");

    // Create the Ontoserver container.
    CreateContainerResponse ontoserverContainer = dockerClient
        .createContainerCmd("aehrc/ontoserver:latest")
        .withEnv(
            "spring.datasource.url=jdbc:postgresql://clinsight-test-ontodb/postgres",
            "authentication.oauth.endpoint.client_id.0=66611fd3-74cb-49de-97f1-662397094b81",
            "authentication.oauth.endpoint.client_secret.0=ecba0d30-b674-45f8-9661-8fbedc3092c0",
            "ontoserver.feature.eclFilters=true",
            "ontoserver.feature.msgaEnabled=true",
            "ontoserver.fhir.batch.queueSize=20000",
            "ontoserver.fhir.too.costly.threshold=200000",
            "JAVA_OPTS=-Xmx24G",
            "ONTOSERVER_INSECURE=true"
        ).withName("clinsight-test-ontoserver")
        .exec();
    createdContainers.add("clinsight-test-ontoserver");

    // Create the Ontoserver database container.
    CreateContainerResponse ontoDatabaseContainer = dockerClient
        .createContainerCmd("postgres")
        .withName("clinsight-test-ontodb")
        .exec();
    createdContainers.add("clinsight-test-ontodb");

    // Connect the test containers to the test network.
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-fhir-server")
        .withNetworkId("clinsight-test")
        .exec();
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-metastore")
        .withNetworkId("clinsight-test")
        .exec();
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-ontoserver")
        .withNetworkId("clinsight-test")
        .exec();
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-ontodb")
        .withNetworkId("clinsight-test")
        .exec();

    // Start the containers.
    dockerClient.startContainerCmd(fhirServerContainer.getId()).exec();
    dockerClient.startContainerCmd(metastoreContainer.getId()).exec();
    dockerClient.startContainerCmd(ontoserverContainer.getId()).exec();
    dockerClient.startContainerCmd(ontoDatabaseContainer.getId()).exec();

    // Wait until the containers reach a healthy state.
    boolean allHealthy = false;
    while (!allHealthy) {
      List<Container> containers = dockerClient.listContainersCmd()
          .withFilter("health", Arrays.asList("healthy"))
          .exec();
      List<String> searchNames = Arrays
          .asList(fhirServerContainer.getId(), ontoserverContainer.getId());
      allHealthy = containers.stream()
          .map(Container::getId)
          .collect(Collectors.toList())
          .containsAll(searchNames);
      if (!allHealthy) {
        Thread.sleep(1000);
      }
    }
  }

  @Test
  public void simpleQuery() {
    assert true;
  }

  @After
  public void tearDown() throws Exception {
    for (String containerId : createdContainers) {
      dockerClient.stopContainerCmd(containerId).exec();
      dockerClient.removeContainerCmd(containerId).exec();
    }
    for (String networkId : createdNetworks) {
      dockerClient.removeNetworkCmd(networkId).exec();
    }
  }
}
