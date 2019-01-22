/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.persistence;

import static au.csiro.clinsight.persistence.Naming.generateRandomKey;

import javax.persistence.Entity;
import javax.persistence.Id;
import org.hibernate.annotations.Type;

/**
 * Describes a set of facts which can be aggregated to produce Metrics that describe the data within
 * a FHIR analytics server.
 *
 * @author John Grimes
 */
@Entity
public class FactSet {

  @Id
  @Type(type = "text")
  private String key;

  @Type(type = "text")
  private String name;

  @Type(type = "text")
  private String title;

  public FactSet() {
    setKey(generateRandomKey());
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

}
