/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.resources;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.hl7.fhir.dstu3.model.Reference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ValueSetDimensionSearchTest {

  private IParser jsonParser;
  private EntityManagerFactory entityManagerFactory;
  private EntityManager entityManager;
  private Dimension dimension;

  @Before
  public void setUp() {
    jsonParser = FhirContext.forDstu3().newJsonParser();
    entityManagerFactory = Persistence.createEntityManagerFactory("au.csiro.clinsight.resources");
    entityManager = entityManagerFactory.createEntityManager();

    Dimension.DescribesComponent describes = new Dimension.DescribesComponent();
    describes.setValueSet("http://clinsight.csiro.au/fhir/ValueSet/my-value-set");

    dimension = new Dimension();
    dimension.setKey("my-dimension");
    dimension.setName("My Dimension");
    dimension.getDescribes().add(describes);
    List<Reference> attributes = new ArrayList<>();
    attributes.add(new Reference("DimensionAttribute/my-dimension-attribute"));
    dimension.setAttribute(attributes);
    dimension.generateJson(jsonParser);

    entityManager.getTransaction().begin();
    entityManager.persist(dimension);
    entityManager.getTransaction().commit();
  }

  @Test
  public void testFindValueSetDimensionByUrl() {
    TypedQuery<Dimension> query = entityManager.createQuery(
        "SELECT d FROM Dimension d JOIN d.describes dd WHERE dd.valueSet = :valueSet",
        Dimension.class);
    query.setParameter("valueSet", "http://clinsight.csiro.au/fhir/ValueSet/my-value-set");
    List<Dimension> results = query.getResultList();
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getKey()).isEqualTo("my-dimension");
  }

  @After
  public void tearDown() {
    entityManager.remove(dimension);
    if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
      entityManagerFactory.close();
    }
  }

}
