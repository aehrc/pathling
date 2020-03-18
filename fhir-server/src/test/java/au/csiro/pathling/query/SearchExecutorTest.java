/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class SearchExecutorTest extends ExecutorTest {

  private ExecutorConfiguration configuration;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Create and configure a new AggregateExecutor.
    Path warehouseDirectory = Files.createTempDirectory("pathling-test-");
    configuration = new ExecutorConfiguration(spark,
        TestUtilities.getFhirContext(), null, null, mockReader);
    configuration.setWarehouseUrl(warehouseDirectory.toString());
    configuration.setDatabaseName("test");
    configuration.setFhirEncoders(FhirEncoders.forR4().getOrCreate());
  }

  @Test
  public void getResourcesWithSingleFilter() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'completed'"));
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(2, 5);
    assertThat(resources.size()).isEqualTo(3);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("542857ec-8ddf-4a4a-bc8c-ffd0a349101b");
    assertThat(resources.get(1).getIdElement().getIdPart())
        .isEqualTo("5a807931-7736-47a7-a250-b7067b461f2b");
    assertThat(resources.get(2).getIdElement().getIdPart())
        .isEqualTo("017e4feb-9b7f-4e5c-b8d8-f09b01145224");
  }

  @Test
  public void getResourcesWithAndFilters() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'active'"));
    filters.addAnd(new StringParam("category.coding contains http://snomed.info/sct|395082007"));
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(0, 100);
    assertThat(resources.size()).isEqualTo(2);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("05fd5547-18ab-406b-9da1-5eea3e787e7f");
    assertThat(resources.get(1).getIdElement().getIdPart())
        .isEqualTo("122ca2d5-d026-4952-929b-8df056a4a157");
  }

  @Test
  public void getResourcesWithOrFilters() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    StringOrListParam orFilters = new StringOrListParam();
    orFilters.addOr(new StringParam("status = 'completed'"));
    orFilters.addOr(new StringParam("category.coding contains http://snomed.info/sct|395082007"));
    filters.addAnd(orFilters);
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(0, 100);
    assertThat(resources.size()).isEqualTo(16);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("01b9ab89-fa4e-40cb-a020-d544b82ea31d");
    assertThat(resources.get(8).getIdElement().getIdPart())
        .isEqualTo("05fd5547-18ab-406b-9da1-5eea3e787e7f");
  }

  @Test
  public void getResourcesWithAndOrFilters() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    StringOrListParam orFilters1 = new StringOrListParam();
    orFilters1.addOr(new StringParam("status = 'completed'"));
    orFilters1.addOr(new StringParam("category.coding contains http://snomed.info/sct|395082007"));
    filters.addAnd(orFilters1);
    StringOrListParam orFilters2 = new StringOrListParam();
    orFilters2.addOr(new StringParam("activity.detail.status contains 'completed'"));
    filters.addAnd(orFilters2);
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(0, 100);
    assertThat(resources.size()).isEqualTo(14);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("017e4feb-9b7f-4e5c-b8d8-f09b01145224");
    assertThat(resources.get(1).getIdElement().getIdPart())
        .isEqualTo("01b9ab89-fa4e-40cb-a020-d544b82ea31d");
  }

  @Test
  public void getResourcesWithNoFilters() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(0, 1);
    assertThat(resources.size()).isEqualTo(1);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("9be4c8a3-b0e8-4301-919a-89da2cfff37d");
  }

  @Test
  public void getResourcesWithNullFilters() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        null);
    List<IBaseResource> resources = searchExecutor.getResources(0, 1);
    assertThat(resources.size()).isEqualTo(1);
    assertThat(resources.get(0).getIdElement().getIdPart())
        .isEqualTo("9be4c8a3-b0e8-4301-919a-89da2cfff37d");
  }

  @Test
  public void size() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'completed'"));
    SearchExecutor searchExecutor = new SearchExecutor(configuration, ResourceType.CAREPLAN,
        filters);
    assertThat(searchExecutor.size()).isEqualTo(14);
  }

  /* TODO: Add test that verifies the behaviour of using "true" as a filter. */

}