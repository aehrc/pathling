package au.csiro.pathling.update;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceWriter;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Create;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.api.MethodOutcome;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.UUID;

import static au.csiro.pathling.fhir.FhirServer.resourceTypeFromClass;

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
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            final String resourceTypeCode = entry.getResource().getResourceType().toString();
            final ResourceType resourceType = ResourceType.fromCode(resourceTypeCode);
            final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceTypeCode);
            final Dataset<IBaseResource> dataset = spark.createDataset(List.of(entry.getResource()), encoder);
            resourceWriter.append(resourceType, dataset);
            System.out.println("Wrote " + entry.getResource() + " to " + resourceTypeCode + " dataset");
        }
        cacheInvalidator.invalidateAll();

        return bundle;
    }

}
