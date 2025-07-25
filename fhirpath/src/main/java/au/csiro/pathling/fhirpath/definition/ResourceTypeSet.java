package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.Collections;
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


  }

  /**
   * Represents a set of declared resource types.
   */
  @Value
  class DeclaredResourceTypes implements ResourceTypeSet {

    @Nonnull
    Set<ResourceType> resourceTypes;

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


  /**
   * Creates a new {@link ResourceTypeSet} from the given resource types. The set must not be
   * empty.
   *
   * @param resourceTypes The resource types
   * @return A new {@link ResourceTypeSet} instance
   */
  static ResourceTypeSet of(@Nonnull final ResourceType... resourceTypes) {
    return new DeclaredResourceTypes(Set.of(resourceTypes));
  }


}
