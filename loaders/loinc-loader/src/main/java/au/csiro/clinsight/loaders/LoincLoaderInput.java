/**
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use
 * is subject to license terms and conditions.
 */
package au.csiro.clinsight.loaders;

public class LoincLoaderInput {

  private String jdbcUrl;
  private String terminologyServerUrl;
  private String loincVersion;
  private String loincUrl = "http://loinc.org";
  private String jdbcDriver = "org.postgresql.Driver";
  private String autoDdl = "update";
  private int expansionPageSize = 2000;

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public String getLoincVersion() {
    return loincVersion;
  }

  public void setLoincVersion(String loincVersion) {
    this.loincVersion = loincVersion;
  }

  public String getLoincUrl() {
    return loincUrl;
  }

  public void setLoincUrl(String loincUrl) {
    this.loincUrl = loincUrl;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public String getAutoDdl() {
    return autoDdl;
  }

  public void setAutoDdl(String autoDdl) {
    this.autoDdl = autoDdl;
  }

  public int getExpansionPageSize() {
    return expansionPageSize;
  }

  public void setExpansionPageSize(int expansionPageSize) {
    this.expansionPageSize = expansionPageSize;
  }

}
