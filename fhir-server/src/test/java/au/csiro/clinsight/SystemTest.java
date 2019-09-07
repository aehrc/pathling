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
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
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
 * These tests run up the Docker container, use a real (local) instance of Apache Spark and a real
 * instance of Ontoserver.
 *
 * @author John Grimes
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "OptionalGetWithoutIsPresent"})
public class SystemTest {

  private static final Logger logger = LoggerFactory.getLogger(SystemTest.class);
  private final List<String> createdNetworks = new ArrayList<>();
  private final List<String> createdContainers = new ArrayList<>();
  private final DockerClient dockerClient;
  private final HttpClient httpClient;
  private final IParser jsonParser;

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
    String path = url.getPath();
    return new File(path).listFiles();
  }

  @Before
  public void setUp() throws Exception {
    // Create a network for the test containers.
    dockerClient.createNetworkCmd()
        .withDriver("bridge")
        .withName("clinsight-test")
        .exec();
    createdNetworks.add("clinsight-test");
    logger.info("Network created");

    // Create the FHIR server container.
    ExposedPort fhirServerPort = ExposedPort.tcp(8090);
    PortBinding fhirServerPortBinding = new PortBinding(Binding.bindPort(8090), fhirServerPort);
    HostConfig fhirServerHostConfig = new HostConfig();
    fhirServerHostConfig.withPortBindings(fhirServerPortBinding);
    CreateContainerResponse fhirServerContainer = dockerClient
        .createContainerCmd("docker-registry.it.csiro.au/clinsight/fhir-server")
        .withExposedPorts(fhirServerPort)
        .withHostConfig(fhirServerHostConfig)
        .withEnv(
            "CLINSIGHT_SPARK_MASTER_URL=local[4]",
            "CLINSIGHT_WAREHOUSE_URL=hdfs://clinsight-test-hdfs:8020/warehouse",
            "CLINSIGHT_DATABASE_NAME=test",
            "CLINSIGHT_EXECUTOR_MEMORY=1g",
            "CLINSIGHT_TERMINOLOGY_SERVER_URL=http://clinsight-test-ontoserver:8080/fhir"
        ).withName("clinsight-test-fhir-server")
        .exec();
    createdContainers.add("clinsight-test-fhir-server");

    // Create the Ontoserver container.
    CreateContainerResponse ontoserverContainer = dockerClient
        .createContainerCmd("aehrc/ontoserver:latest")
        .withEnv(
            "spring.datasource.url=jdbc:postgresql://clinsight-test-ontodb/postgres",
            "authentication.oauth.endpoint.client_id.0=66611fd3-74cb-49de-97f1-662397094b81",
            "authentication.oauth.endpoint.client_secret.0=ecba0d30-b674-45f8-9661-8fbedc3092c0",
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

    // Create the HDFS namenode container.
    ExposedPort hdfsPort = ExposedPort.tcp(8020);
    ExposedPort hdfsWebPort = ExposedPort.tcp(50070);
    PortBinding hdfsPortBinding = new PortBinding(Binding.bindPort(8020), hdfsPort);
    PortBinding hdfsWebPortBinding = new PortBinding(Binding.bindPort(50070), hdfsWebPort);
    HostConfig hdfsHostConfig = new HostConfig();
    hdfsHostConfig.withPortBindings(hdfsPortBinding, hdfsWebPortBinding);
    CreateContainerResponse hdfsContainer = dockerClient
        .createContainerCmd("uhopper/hadoop-namenode:2.7.2")
        .withExposedPorts(hdfsPort, hdfsWebPort)
        .withHostConfig(hdfsHostConfig)
        .withEnv(
            "CLUSTER_NAME=test",
            "CORE_CONF_fs_default_name=hdfs://clinsight-test-hdfs:8020",
            "HDFS_CONF_dfs_permissions_enabled=false",
            "HDFS_CONF_dfs_client_use_datanode_hostname=true"
        ).withName("clinsight-test-hdfs")
        .exec();
    createdContainers.add("clinsight-test-hdfs");

    // Create the HDFS datanode container.
    ExposedPort hdfsDatanodePort = ExposedPort.tcp(50075);
    ExposedPort hdfsDatanodeTransferPort = ExposedPort.tcp(50010);
    ExposedPort hdfsDatanodeIpcPort = ExposedPort.tcp(50020);
    PortBinding hdfsDatanodePortBinding = new PortBinding(Binding.bindPort(50075),
        hdfsDatanodePort);
    PortBinding hdfsDatanodeTransferPortBinding = new PortBinding(Binding.bindPort(50010),
        hdfsDatanodeTransferPort);
    PortBinding hdfsDatanodeIpcPortBinding = new PortBinding(Binding.bindPort(50020),
        hdfsDatanodeIpcPort);
    HostConfig hdfsDatanodeHostConfig = new HostConfig();
    hdfsDatanodeHostConfig
        .withPortBindings(hdfsDatanodePortBinding, hdfsDatanodeTransferPortBinding,
            hdfsDatanodeIpcPortBinding);
    CreateContainerResponse hdfsDatanodeContainer = dockerClient
        .createContainerCmd("uhopper/hadoop-datanode:2.7.2")
        .withExposedPorts(hdfsDatanodePort, hdfsDatanodeTransferPort, hdfsDatanodeIpcPort)
        .withHostConfig(hdfsDatanodeHostConfig)
        .withEnv(
            "CORE_CONF_fs_default_name=hdfs://clinsight-test-hdfs:8020",
            "HDFS_CONF_dfs_permissions_enabled=false",
            "HDFS_CONF_dfs_datanode_hostname=localhost"
        ).withName("clinsight-test-hdfs-datanode")
        .exec();
    createdContainers.add("clinsight-test-hdfs-datanode");

    // Connect the test containers to the test network.
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-fhir-server")
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
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-hdfs")
        .withNetworkId("clinsight-test")
        .exec();
    dockerClient.connectToNetworkCmd()
        .withContainerId("clinsight-test-hdfs-datanode")
        .withNetworkId("clinsight-test")
        .exec();

    // Start the containers.
    dockerClient.startContainerCmd(fhirServerContainer.getId()).exec();
    dockerClient.startContainerCmd(ontoserverContainer.getId()).exec();
    dockerClient.startContainerCmd(ontoDatabaseContainer.getId()).exec();
    dockerClient.startContainerCmd(hdfsContainer.getId()).exec();
    dockerClient.startContainerCmd(hdfsDatanodeContainer.getId()).exec();
    logger.info("All containers started");

    // Wait until the containers reach a healthy state.
    boolean allHealthy = false;
    logger.info("Waiting for containers to achieve healthy state");
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
    logger.info("All containers healthy");

    // Save the test resources into HDFS.
    logger.info("Loading test data into HDFS");
    Configuration hadoopConfiguration = new Configuration();
    String stagingUri = "hdfs://localhost:8020/staging";
    FileSystem hdfs = FileSystem.get(new URI(stagingUri), hadoopConfiguration);
    for (File testFile : getResourceFolderFiles("test-data/fhir")) {
      Path path = new Path(stagingUri + "/" + testFile.getName());
      logger.debug("Creating: " + path.toUri().toString());
      OutputStream hdfsOutputStream = hdfs.create(path);
      InputStream fileInputStream = new FileInputStream(testFile);
      org.apache.hadoop.io.IOUtils
          .copyBytes(fileInputStream, hdfsOutputStream, hadoopConfiguration);
      logger.debug("Created: " + path.toUri().toString());
    }
    logger.info("HDFS data load complete");
  }

  @Test
  public void importDataAndQuery() throws IOException {
    Parameters inParams = new Parameters();

    // Set subject resource parameter.
    ParametersParameterComponent subjectResourceParam = new ParametersParameterComponent();
    subjectResourceParam.setName("subjectResource");
    subjectResourceParam.setValue(new UriType("http://hl7.org/fhir/StructureDefinition/Patient"));

    // Add aggregation, number of patients.
    ParametersParameterComponent aggregationParam = new ParametersParameterComponent();
    aggregationParam.setName("aggregation");
    ParametersParameterComponent aggregationExpression = new ParametersParameterComponent();
    aggregationExpression.setName("expression");
    aggregationExpression.setValue(new StringType("%resource.count"));
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
        "%resource.reverseResolve(Condition.subject)"
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
    filterParam.setValue(new StringType("%resource.gender = 'female'"));

    inParams.getParameter().add(subjectResourceParam);
    inParams.getParameter().add(aggregationParam);
    inParams.getParameter().add(groupingParam);
    inParams.getParameter().add(filterParam);

    // Send a request to the `$query` operation on the FHIR server.
    String requestString = jsonParser.encodeResourceToString(inParams);
    HttpPost httpPost = new HttpPost("http://localhost:8090/fhir/$query");
    httpPost.setEntity(new StringEntity(requestString));
    httpPost.addHeader("Content-Type", "application/fhir+json");
    httpPost.addHeader("Accept", "application/fhir+json");

    String responseString;
    try (CloseableHttpResponse response = (CloseableHttpResponse) httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
      responseString = writer.toString();
    }
    Parameters outParams = (Parameters) jsonParser.parseResource(responseString);

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
  }

  @After
  public void tearDown() {
    logger.info("Stopping containers");
    for (String containerId : createdContainers) {
      dockerClient.stopContainerCmd(containerId).exec();
      dockerClient.removeContainerCmd(containerId).exec();
    }
    logger.info("Containers stopped, removing network");
    for (String networkId : createdNetworks) {
      dockerClient.removeNetworkCmd(networkId).exec();
    }
    logger.info("Network removed, teardown complete");
  }
}
