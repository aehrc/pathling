/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.dstu3.model.Reference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @author John Grimes
 */
public class DimensionPersistenceTest {

    private IParser jsonParser;
    private EntityManagerFactory entityManagerFactory;
    private EntityManager entityManager;
    private Dimension dimension;

    @Before
    public void setUp() {
        jsonParser = FhirContext.forDstu3().newJsonParser();
        entityManagerFactory = Persistence.createEntityManagerFactory("au.csiro.clinsight.persistence");
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
    public void testFindDimensionById() {
        Dimension result = entityManager.find(Dimension.class, "my-dimension");
        result.populateFromJson(jsonParser);
        assertThat(result.getName()).isEqualTo("My Dimension");
        assertThat(result.getAttribute()
                         .get(0)
                         .getReference()).isEqualTo("DimensionAttribute/my-dimension-attribute");
        assertThat(result.getDescribes().get(0).getValueSet())
                .isEqualTo("http://clinsight.csiro.au/fhir/ValueSet/my-value-set");
    }

    @Test
    public void testFindAllDimensions() {
        TypedQuery<Dimension> query = entityManager.createQuery("SELECT d FROM Dimension d", Dimension.class);
        List<Dimension> results = query.getResultList();
        assertThat(results.size()).isEqualTo(1);
        assertThat(results.get(0).getKey()).isEqualTo("my-dimension");
    }

    @After
    public void tearDown() {
        entityManager.remove(dimension);
        if (entityManagerFactory != null && entityManagerFactory.isOpen())
            entityManagerFactory.close();
    }

}
