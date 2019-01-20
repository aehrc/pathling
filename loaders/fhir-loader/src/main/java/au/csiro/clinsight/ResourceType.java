/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */

package au.csiro.clinsight;

import java.util.Objects;

public class ResourceType {

  private String resourceName;
  private String profileUri;

  public ResourceType(String resourceName, String profileUri) {
    this.resourceName = resourceName;
    this.profileUri = profileUri;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getProfileUri() {
    return profileUri;
  }

  public void setProfileUri(String profileUri) {
    this.profileUri = profileUri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResourceType)) {
      return false;
    }
    ResourceType that = (ResourceType) o;
    return Objects.equals(resourceName, that.resourceName) &&
        Objects.equals(profileUri, that.profileUri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceName, profileUri);
  }

}
