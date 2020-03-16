package au.csiro.pathling.encoding;

import java.io.Serializable;
import org.hl7.fhir.r4.model.Coding;

public class SimpleCoding implements Serializable {

  private static final long serialVersionUID = 1L;

  private String system;
  private String code;
  private String version;


  public SimpleCoding(String system, String code, String version) {
    this.system = system;
    this.code = code;
    this.version = version;
  }

  public SimpleCoding(String system, String code) {
    this(system, code, null);
  }

  public SimpleCoding() {}

  public SimpleCoding(Coding coding) {
    this.system = coding.getSystem();
    this.code = coding.getCode();
    this.version = coding.getVersion();
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

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Coding toCoding() {
    return new Coding(system, code, version);
  }

  public boolean isNull() {
    return system == null && code == null;
  }

  public boolean isNotNull() {
    return !isNull();
  }

  public boolean matches(SimpleCoding other) {
    if (this == other)
      return true;
    if (null == other)
      return false;

    if (this.version != null && other.version != null) {
      return this.equals(other);
    }

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


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((code == null) ? 0 : code.hashCode());
    result = prime * result + ((system == null) ? 0 : system.hashCode());
    result = prime * result + ((version == null) ? 0 : version.hashCode());
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
    SimpleCoding other = (SimpleCoding) obj;
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
    if (version == null) {
      if (other.version != null)
        return false;
    } else if (!version.equals(other.version))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SimpleCoding [system=" + system + ", code=" + code + ", version=" + version + "]";
  }



}
