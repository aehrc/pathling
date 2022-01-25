package au.csiro.pathling.update;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Resource;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

import static org.hl7.fhir.r4.model.Bundle.BundleType.BATCHRESPONSE;

/**
 * HAPI plain provider that provides update operations with a system-wide batch submission as a bundle.
 *
 * @author Sean Fong
 */
@Component
public class BatchProvider {
  @Nonnull
  private final UpdateHelpers updateHelpers;

  @Nonnull
  private final CacheInvalidator cacheInvalidator;

  public BatchProvider(@Nonnull final UpdateHelpers updateHelpers,
      @Nonnull final CacheInvalidator cacheInvalidator) {
    this.updateHelpers = updateHelpers;
    this.cacheInvalidator = cacheInvalidator;
  }

  @Transaction
  @OperationAccess("batch")
  public Bundle batch(@TransactionParam Bundle bundle) {
    // Build response bundle
    Bundle transactionResponse = new Bundle();
    transactionResponse.setId(UUID.randomUUID().toString());
    transactionResponse.setType(BATCHRESPONSE);
    transactionResponse.addLink()
            .setRelation("self")
            .setUrl("http://localhost:8080/fhir");

    // Add resources to their respective datasets
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      final Resource resource = entry.getResource();

      // Parse urn uuid
      final String urnUuid = "urn:uuid:";
      if (resource.getId().startsWith(urnUuid)) {
        resource.setId(resource.getId().substring(urnUuid.length()));
      }

      // Append or update dataset based on request method
      final String resourceTypeCode = resource.getResourceType().toString();
      final ResourceType resourceType = ResourceType.fromCode(resourceTypeCode);
      final String requestMethod = entry.getRequest().getMethod().toString();
      final Bundle.BundleEntryResponseComponent resourceResponse = new Bundle.BundleEntryResponseComponent();

      if (requestMethod.equals("POST") || requestMethod.equals("PUT")) {
        if (requestMethod.equals("POST")) {
          updateHelpers.appendDataset(resourceType, resource);
        } else {
          updateHelpers.updateDataset(resourceType, resource);
        }
        // Hardcode response entry for now
        resourceResponse.setStatus("200 OK")
                .setLocation(resourceTypeCode + "/" + resource.getId())
                .setEtag("1");
        transactionResponse.addEntry().setResponse(resourceResponse);

      } else {
        // Bad request
      }
    }
    cacheInvalidator.invalidateAll();

    return transactionResponse;
  }

}
