package au.csiro.pathling.encoding;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private List<SystemAndCode> leftCodings;
  private List<SystemAndCode> rightCodings;
  
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public List<SystemAndCode> getLeftCodings() {
    return leftCodings;
  }
  public void setLeftCodings(List<SystemAndCode> leftCodings) {
    this.leftCodings = leftCodings;
  }
  public List<SystemAndCode> getRightCodings() {
    return rightCodings;
  }
  public void setRightCodings(List<SystemAndCode> rightCodings) {
    this.rightCodings = rightCodings;
  }
  
  public boolean subsumes() {
     Set<SystemAndCode> ls = new HashSet<SystemAndCode>(rightCodings);
     ls.retainAll(leftCodings);
     return !ls.isEmpty();
  }
}
