package au.csiro.pathling.fhirpath.collection.mixed;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.execution.ResolvingFhirPathEvaluator.FhirpathResult;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a polymorphic resource collection, which can be resolved to a single resource type.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Value
@EqualsAndHashCode(callSuper = false)
public class MixedResourceCollection extends MixedCollection {

  @FunctionalInterface
  public interface Purifier {

    @Nonnull
    FhirpathResult apply(@Nonnull final ResourceType type, @Nonnull final FhirPath childPath);
  }
  
  @Nonnull
  Purifier purifier;

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   * type
   */
  @Nonnull
  @Override
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    throw new UnsupportedOperationException("filterByType is not supported for MixedResourceCollection");
  }

}
