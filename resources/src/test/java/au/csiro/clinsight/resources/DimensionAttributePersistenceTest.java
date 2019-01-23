/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.resources;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Reference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class DimensionAttributePersistenceTest {

  private IParser jsonParser;
  private EntityManagerFactory entityManagerFactory;
  private EntityManager entityManager;
  private DimensionAttribute dimensionAttribute;

  @Before
  public void setUp() {
    jsonParser = FhirContext.forDstu3().newJsonParser();
    entityManagerFactory = Persistence.createEntityManagerFactory("au.csiro.clinsight.resources");
    entityManager = entityManagerFactory.createEntityManager();

    dimensionAttribute = new DimensionAttribute();
    dimensionAttribute.setKey("my-dimension-attribute");
    dimensionAttribute.setName("My Dimension Attribute");
    dimensionAttribute
        .setType(new Coding("http://hl7.org/au.csiro.clinsight.fhir/ValueSet/defined-types",
            "string",
            "string"));
    dimensionAttribute.setDimension(new Reference("Dimension/my-dimension"));
    dimensionAttribute.generateJson(jsonParser);

    entityManager.getTransaction().begin();
    entityManager.persist(dimensionAttribute);
    entityManager.getTransaction().commit();
  }

  @Test
  public void testFindDimensionAttributeById() {
    DimensionAttribute result = entityManager
        .find(DimensionAttribute.class, "my-dimension-attribute");
    result.populateFromJson(jsonParser);
    assertThat(result.getName()).isEqualTo("My Dimension Attribute");
    assertThat(result.getType().getCode()).isEqualTo("string");
    assertThat(result.getDimension()
        .getReference()).isEqualTo("Dimension/my-dimension");
  }

  @Test
  public void testFindAllDimensionAttributes() {
    TypedQuery<DimensionAttribute> query = entityManager
        .createQuery("SELECT da FROM DimensionAttribute da",
            DimensionAttribute.class);
    List<DimensionAttribute> results = query.getResultList();
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getKey()).isEqualTo("my-dimension-attribute");
  }

  @After
  public void tearDown() {
    entityManager.remove(dimensionAttribute);
    if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
      entityManagerFactory.close();
    }
  }

}