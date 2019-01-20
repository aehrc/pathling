/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import au.csiro.clinsight.fhir.FhirServer;
import au.csiro.clinsight.persistence.Dimension;
import au.csiro.clinsight.persistence.DimensionAttribute;
import au.csiro.clinsight.persistence.FactSet;
import au.csiro.clinsight.persistence.Metric;
import au.csiro.clinsight.persistence.Query;
import au.csiro.clinsight.persistence.QueryResult;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hl7.fhir.instance.model.api.IBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

/**
 * @author John Grimes
 */
@SpringBootApplication
public class AnalyticsServer {

  public static void main(String[] args) {
    SpringApplication.run(AnalyticsServer.class, args);
  }

  @Bean
  public FhirContext fhirContext() {
    FhirContext fhirContext = FhirContext.forDstu3();

    // Declare custom types.
    List<Class<? extends IBase>> customTypes = new ArrayList<>();
    customTypes.add(Dimension.class);
    customTypes.add(DimensionAttribute.class);
    customTypes.add(Metric.class);
    customTypes.add(Query.class);
    customTypes.add(QueryResult.class);
    customTypes.add(QueryResult.LabelComponent.class);
    customTypes.add(QueryResult.DataComponent.class);
    fhirContext.registerCustomTypes(customTypes);

    return fhirContext;
  }

  @Bean
  @Autowired
  public ServletRegistrationBean<FhirServer> servletRegistrationBean(FhirServer fhirServer) {
    ServletRegistrationBean<FhirServer> servletRegistrationBean =
        new ServletRegistrationBean<>(fhirServer, "/fhir/*");
    servletRegistrationBean.setName("FHIR");
    return servletRegistrationBean;
  }

  @Bean
  public SessionFactory sessionFactory(@Value("${clinsight.jdbcUrl}") String jdbcUrl) {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.url", jdbcUrl);
    properties.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    properties.setProperty("hibernate.hbm2ddl.auto", "update");
    properties.setProperty("hibernate.jdbc.lob.non_contextual_creation", "true");
    properties.setProperty("hibernate.show_sql", "true");
    Configuration configuration = new Configuration()
        .addAnnotatedClass(Dimension.class)
        .addAnnotatedClass(Dimension.DescribesComponent.class)
        .addAnnotatedClass(DimensionAttribute.class)
        .addAnnotatedClass(Metric.class)
        .addAnnotatedClass(FactSet.class)
        .setProperties(properties);
    return configuration.buildSessionFactory();
  }

  @Bean
  @Autowired
  public Session session(SessionFactory sessionFactory) {
    // TODO: Look at current session context, and which would be the most appropriate to use here.
    return sessionFactory.openSession();
  }

}
