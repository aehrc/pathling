package au.csiro.pathling.encoding;

import java.io.Serializable;
import org.hl7.fhir.r4.model.Coding;

public class SystemAndCode implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  private String system;
  private String code;
  
  public SystemAndCode(String system, String code) {
    this.system = system;
    this.code = code;
  }
  
  public SystemAndCode() {
  }
  
  public SystemAndCode(Coding coding) {
    super();
    this.system = coding.getSystem();
    this.code = coding.getCode();
  }
  public String getSystem() {
    return system;
  }
  public void setSystem(String system) {
    this.system = system;
  }
  public String getCode() {
    return code;
  }
  public void setCode(String code) {
    this.code = code;
  }

  public Coding toCoding() {
    return new Coding(system, code, null);
  }
  
  
  public boolean isNull() {
    return system == null && code == null;
  }

  public boolean isNotNull() {
    return !isNull();
  }
  
  @Override
  public String toString() {
    return "SimpleCode [system=" + system + ", code=" + code + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((code == null) ? 0 : code.hashCode());
    result = prime * result + ((system == null) ? 0 : system.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SystemAndCode other = (SystemAndCode) obj;
    if (code == null) {
      if (other.code != null)
        return false;
    } else if (!code.equals(other.code))
      return false;
    if (system == null) {
      if (other.system != null)
        return false;
    } else if (!system.equals(other.system))
      return false;
    return true;
  }
  
  
}