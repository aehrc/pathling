package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a set of resource types.
 */
public interface ResourceTypeSet {

  /**
   * Returns true if this set contains the given resource type.
   *
   * @param resourceType The resource type to check for
   * @return true if the resource type is in this set
   */
  boolean contains(@Nonnull final ResourceType resourceType);

  /**
   * Returns a single resource type if this set contains exactly one resource type.
   *
   * @return An optional containing the single resource type, or empty if there are multiple
   * resource types
   */
  @Nonnull
  Optional<ResourceType> asSingleResourceType();

  /**
   * Represents a set of all possible resource types.
   */
  class AllResourceTypes implements ResourceTypeSet {

    private static final AllResourceTypes INSTANCE = new AllResourceTypes();

    private AllResourceTypes() {
    }

    @Override
    public boolean contains(@Nonnull final ResourceType resourceType) {
      return true;
    }

    @Override
    public String toString() {
      return "[*]";
    }


    @Override
    @Nonnull
    public Optional<ResourceType> asSingleResourceType() {
      return Optional.empty();
    }
  }

  /**
   * Represents a set of declared resource types.
   */
  class DeclaredResourceTypes implements ResourceTypeSet {

    @Nonnull
    private final Set<ResourceType> resourceTypes;

    private DeclaredResourceTypes(@Nonnull final Set<ResourceType> resourceTypes) {
      if (resourceTypes.isEmpty()) {
        throw new IllegalArgumentException("Declared resource types must not be empty");
      }
      this.resourceTypes = Collections.unmodifiableSet(resourceTypes);
    }

    @Override
    public boolean contains(@Nonnull final ResourceType resourceType) {
      return resourceTypes.contains(resourceType);
    }

    @Override
    @Nonnull
    public Optional<ResourceType> asSingleResourceType() {
      return resourceTypes.size() == 1
             ? Optional.of(resourceTypes.iterator().next())
             : Optional.empty();
    }

    @Override
    public String toString() {
      return resourceTypes.toString();
    }
  }

  /**
   * Creates a new {@link ResourceTypeSet} from the given set of resource types. Treats an empty set
   * as equivalent to all resource types.
   *
   * @param resourceTypes The set of resource types
   * @return A new {@link ResourceTypeSet} instance
   */
  static ResourceTypeSet from(@Nonnull final Set<ResourceType> resourceTypes) {
    return resourceTypes.isEmpty()
           ? allResourceTypes()
           : of(resourceTypes);
  }


  /**
   * Returns a set of all possible resource types.
   *
   * @return A set of all possible resource types
   */
  static ResourceTypeSet allResourceTypes() {
    return AllResourceTypes.INSTANCE;
  }

  /**
   * Creates a new {@link ResourceTypeSet} from the given set of resource types. The set must not be
   * empty.
   *
   * @param resourceTypes The set of resource types
   * @return A new {@link ResourceTypeSet} instance
   */
  static ResourceTypeSet of(@Nonnull final Set<ResourceType> resourceTypes) {
    return new DeclaredResourceTypes(resourceTypes);
  }

}
