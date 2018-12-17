/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.persistence;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @author John Grimes
 */
public class MetricPersistenceTest {

    private IParser jsonParser;
    private EntityManagerFactory entityManagerFactory;
    private EntityManager entityManager;
    private Metric metric;

    @Before
    public void setUp() {
        jsonParser = FhirContext.forDstu3().newJsonParser();
        entityManagerFactory = Persistence.createEntityManagerFactory("au.csiro.clinsight.persistence");
        entityManager = entityManagerFactory.createEntityManager();

        FactSet factSet = new FactSet();
        factSet.setName("Some Fact Set");

        metric = new Metric();
        metric.setKey("my-metric");
        metric.setName("My Metric");
        metric.setFactSet(factSet);
        metric.generateJson(jsonParser);

        entityManager.getTransaction().begin();
        entityManager.persist(factSet);
        entityManager.persist(metric);
        entityManager.getTransaction().commit();
    }

    @Test
    public void testFindMetricById() {
        Metric result = entityManager.find(Metric.class, "my-metric");
        result.populateFromJson(jsonParser);
        assertThat(result.getName()).isEqualTo("My Metric");
        assertThat(result.getFactSet().getName().equals("Some Fact Set"));
    }

    @Test
    public void testFindAllMetrics() {
        TypedQuery<Metric> query = entityManager.createQuery("SELECT m FROM Metric m", Metric.class);
        List<Metric> results = query.getResultList();
        assertThat(results.size()).isEqualTo(1);
        assertThat(results.get(0).getKey()).isEqualTo("my-metric");
    }

    @After
    public void tearDown() {
        entityManager.remove(metric);
        if (entityManagerFactory != null && entityManagerFactory.isOpen())
            entityManagerFactory.close();
    }

}