package au.csiro.pathling.fhirpath.execution;

import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public interface JoinTag {

  @Nonnull
  String getTag();
  
  static boolean isJoinTag(@Nonnull final String name) {
    return name.contains("@");
  }
  
  @Value
  class ResourceTag implements JoinTag {

    @Nonnull
    ResourceType resourceType;

    @Override
    @Nonnull
    public String getTag() {
      return resourceType.toCode();
    }
  }

  @Value(staticConstructor = "of")
  class ResolveTag implements JoinTag {

    @Nonnull
    ResourceType childResourceType;

    @Override
    @Nonnull
    public String getTag() {
      return "id" + "@" + childResourceType.toCode();
    }
  }

  @Value(staticConstructor = "of")
  class ReverseResolveTag implements JoinTag {

    @Nonnull
    ResourceType childResourceType;

    @Nonnull
    String masterKeyPath;

    @Override
    @Nonnull
    public String getTag() {
      return childResourceType.toCode() + "@" + masterKeyPath.replace(".", "_");
    }
  }

}
