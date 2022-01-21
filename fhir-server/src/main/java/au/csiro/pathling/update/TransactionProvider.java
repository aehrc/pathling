package au.csiro.pathling.update;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.util.BundleBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IdType;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;
import static org.hl7.fhir.r4.model.Bundle.BundleType.TRANSACTIONRESPONSE;

/**
 * HAPI plain provider that provides update operations with a transaction.
 *
 * @author Sean Fong
 */
@Component
public class TransactionProvider {
    @Nonnull
    private final SparkSession spark;

    @Nonnull
    private final FhirEncoders fhirEncoders;

    @Nonnull
    private final ResourceWriter resourceWriter;

    @Nonnull
    private final CacheInvalidator cacheInvalidator;

    public TransactionProvider(@Nonnull final SparkSession spark,
                               @Nonnull final FhirEncoders fhirEncoders,
                               @Nonnull final ResourceWriter resourceWriter,
                               @Nonnull final CacheInvalidator cacheInvalidator) {
        this.spark = spark;
        this.fhirEncoders = fhirEncoders;
        this.resourceWriter = resourceWriter;
        this.cacheInvalidator = cacheInvalidator;
    }

    @Transaction
    @OperationAccess("transaction")
    public Bundle transaction(@TransactionParam Bundle bundle) {

        // Build response bundle
        Bundle transactionResponse = new Bundle();
        transactionResponse.setId(UUID.randomUUID().toString());
        transactionResponse.setType(TRANSACTIONRESPONSE);
        transactionResponse.addLink()
                .setRelation("self")
                .setUrl("http://localhost:8080/fhir");

        // Add resources to their respective datasets
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            final String resourceIdCode = entry.getResource().getId();
            final String resourceTypeCode = entry.getResource().getResourceType().toString();
            final ResourceType resourceType = ResourceType.fromCode(resourceTypeCode);
            final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceTypeCode);
            final Dataset<IBaseResource> dataset = spark.createDataset(List.of(entry.getResource()), encoder);

            // Hardcode response entry for now
            Bundle.BundleEntryResponseComponent resourceResponse = new Bundle.BundleEntryResponseComponent();
            resourceResponse.setStatus("200 OK")
                    .setLocation(resourceTypeCode + "/" + resourceIdCode)
                    .setEtag("1");
            transactionResponse.addEntry().setResponse(resourceResponse);

            resourceWriter.append(resourceType, dataset);
        }
        cacheInvalidator.invalidateAll();

        return transactionResponse;
    }

}
