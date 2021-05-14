package au.csiro.pathling.security;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.ResourceProviderFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.search.CachingSearchProvider;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.update.ImportProvider;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;


@ActiveProfiles({"core", "server"})
public class SecurityTestForOperations extends SecurityTest {

  @Autowired
  private ImportProvider importProvider;

  @Autowired
  private ResourceProviderFactory resourceProviderFactory;

  @MockBean
  private ResourceReader resourceReader;

  @MockBean
  private ResourceWriter resourceWriter;

  @Autowired
  private SparkSession sparkSession;

  @BeforeEach
  public void setUp() {
    when(resourceReader.read(any()))
        .thenReturn(new ResourceDatasetBuilder(sparkSession).withIdColumn().build());
  }

  public void assertImportSuccess() {
    try {
      importProvider.importOperation(new Parameters());
    } catch (InvalidUserInputError ex) {
      // pass
    }
  }

  public void assertAggregateSuccess() {
    final AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    try {
      aggregateProvider.aggregate(null, null, null);
    } catch (InvalidUserInputError ex) {
      // pass
    }
  }

  public void assertSearchSuccess() {
    final SearchProvider searchProvider = (SearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, false);
    searchProvider.search(null);
  }

  public void assertSearchWithFilterSuccess() {
    final SearchProvider searchProvider = (SearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, false);
    searchProvider.search(null);
  }

  public void assertCachingSearchSuccess() {
    final CachingSearchProvider cachingSearchProvider = (CachingSearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, true);
    cachingSearchProvider.search();
    cachingSearchProvider.search(null);
  }

  public void assertCachingSearchWithFilterSuccess() {
    final CachingSearchProvider cachingSearchProvider = (CachingSearchProvider) resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient, true);
    cachingSearchProvider.search(null);
  }
}
