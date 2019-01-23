/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import static au.csiro.clinsight.resources.Naming.tableNameForDimension;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.resources.Dimension;
import au.csiro.clinsight.resources.DimensionAttribute;
import ca.uhn.fhir.context.FhirContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.persistence.TypedQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Type;
import org.hl7.fhir.instance.model.api.IBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoincLoader {

  private static final Logger logger = LoggerFactory.getLogger(LoincLoader.class);
  private static final Map<String, String> classTypeToCategory = new HashMap<>() {{
    put("1", "Laboratory class");
    put("2", "Clinical class");
    put("3", "Claims attachments");
    put("4", "Surveys");
  }};
  private final FhirContext fhirContext;
  private Connection connection;
  private SessionFactory sessionFactory;
  private TerminologyClient terminologyClient;

  public LoincLoader(String jdbcUrl, String terminologyServerUrl, String jdbcDriver, String autoDdl)
      throws SQLException {
    initialiseSessionFactory(jdbcUrl, jdbcDriver, autoDdl);
    connection = DriverManager.getConnection(jdbcUrl);
    fhirContext = initialiseFhirContext();
    terminologyClient = new TerminologyClient(fhirContext, terminologyServerUrl);
  }

  private void initialiseSessionFactory(String jdbcUrl, String jdbcDriver, String autoDdl) {
    Properties properties = new Properties();
    properties.setProperty("hibernate.connection.driver_class", jdbcDriver);
    properties.setProperty("hibernate.hbm2ddl.auto", autoDdl);
    properties.setProperty("hibernate.connection.url", jdbcUrl);
    Configuration configuration = new Configuration()
        .addAnnotatedClass(Dimension.class)
        .addAnnotatedClass(Dimension.DescribesComponent.class)
        .addAnnotatedClass(DimensionAttribute.class)
        .setProperties(properties);
    sessionFactory = configuration.buildSessionFactory();
  }

  private FhirContext initialiseFhirContext() {
    FhirContext fhirContext;
    fhirContext = FhirContext.forDstu3();

    // Declare custom types.
    List<Class<? extends IBase>> customTypes = new ArrayList<>();
    customTypes.add(Dimension.class);
    customTypes.add(DimensionAttribute.class);

    fhirContext.registerCustomTypes(customTypes);
    return fhirContext;
  }

  void execute(String loincUrl, String loincVersion, int expansionPageSize) throws SQLException {
    long start = System.nanoTime();
    LoincMetadataWriter metadataWriter = new LoincMetadataWriter(fhirContext,
        sessionFactory,
        loincUrl,
        loincVersion);
    // TODO: Use the JDBC API for this.
    execute("START TRANSACTION");
    try {
      if (!dimensionExists(loincUrl, loincVersion)) {
        ensureTableExists(tableNameForDimension(metadataWriter.getDimension()));
        LoincCodes loincCodes = new LoincCodes(terminologyClient, expansionPageSize);
        PreparedStatement insertStatement =
            createCodeInsertStatement(tableNameForDimension(metadataWriter.getDimension()));
        for (LoincCode loincCode : loincCodes) {
          List<String> values = prepareValues(insertStatement, loincCode);
          logger.debug("Prepared to insert values: " + String.join(", ", values));
          execute(insertStatement);
          logger.info("[" + loincCodes.getCurrentIndex() + " of " + loincCodes.getTotal() +
              "] Processed " + values.get(0) + " | " + values.get(1));
        }
        // TODO: Use the JDBC API for this.
        execute("COMMIT");
        double elapsedSecs = TimeUnit.SECONDS
            .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        logger.info("Built LOINC code dimension in " + String.format("%.1f", elapsedSecs) + " s");
        metadataWriter.write();
      } else {
        logger.info(
            "LOINC Dimension already exists, skipping load: " + loincUrl + ", " + loincVersion);
      }
    } finally {
      sessionFactory.close();
      connection.close();
    }
  }

  private boolean dimensionExists(String loincUrl, String loincVersion) {
    Session session = sessionFactory.openSession();
    try {
      TypedQuery<Dimension> query;
      if (loincVersion == null) {
        query = session.createQuery(
            "SELECT d FROM Dimension d JOIN d.describes dd WHERE dd.codeSystem.url = :url",
            Dimension.class);
      } else {
        query = session.createQuery(
            "SELECT d FROM Dimension d JOIN d.describes dd WHERE dd.codeSystem.url = :url AND dd" +
                ".codeSystem.version = :version",
            Dimension.class);
        query.setParameter("version", loincVersion);
      }
      query.setParameter("url", loincUrl);
      return query.getResultStream().findFirst().isPresent();
    } finally {
      session.close();
    }
  }

  private void ensureTableExists(String tableName) throws SQLException {
    execute("DROP TABLE IF EXISTS " + tableName + " CASCADE");
    execute("CREATE TABLE " + tableName + " (" +
        // TODO: Make this consistent with other table keys in the data mart.
        "key serial PRIMARY KEY, " +
        "code text UNIQUE, " +
        "longName text, " +
        "fsn text, " +
        "status text, " +
        "component text, " +
        "method text, " +
        "property text, " +
        "scale text, " +
        "system text, " +
        "timeAspect text, " +
        "category text, " +
        "answerList text, " +
        "parent text, " +
        "child text, " +
        "class text, " +
        "exampleUcum text, " +
        "orderObservation text, " +
        "consumerFriendlyName text " +
        ")");
  }

  private PreparedStatement createCodeInsertStatement(String tableName) throws SQLException {
    return connection.prepareStatement("INSERT INTO " + tableName + " (" +
        "code, " +
        "longName, " +
        "fsn, " +
        "status, " +
        "component, " +
        "method, " +
        "property, " +
        "scale, " +
        "system, " +
        "timeAspect, " +
        "category, " +
        "answerList, " +
        "parent, " +
        "child, " +
        "class, " +
        "exampleUcum, " +
        "orderObservation, " +
        "consumerFriendlyName" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
  }

  private List<String> prepareValues(PreparedStatement insertStatement, LoincCode loincCode)
      throws SQLException {
    List<String> values = Arrays.asList(
        loincCode.getExpansionComponent().getCode(),
        // LOINC Code
        loincCode.getExpansionComponent().getDisplay(),
        // LOINC Long Name
        getDesignationValue(loincCode, "900000000000003001"),
        // LOINC Fully Specified Name
        getPropertyValue(loincCode, "STATUS"),
        // LOINC Code Status
        getPropertyValue(loincCode, "COMPONENT"),
        // LOINC Component
        getPropertyValue(loincCode, "METHOD_TYP"),
        // LOINC Method
        getPropertyValue(loincCode, "PROPERTY"),
        // LOINC Property
        getPropertyValue(loincCode, "SCALE_TYP"),
        // LOINC Scale
        getPropertyValue(loincCode, "SYSTEM"),
        // LOINC System
        getPropertyValue(loincCode, "TIME_ASPCT"),
        // LOINC Time Aspect
        classTypeToCategory.get(getPropertyValue(loincCode, "CLASSTYPE")),
        // LOINC Code Category
        getPropertyValue(loincCode, "answer-list"),
        // LOINC Answer List Code
        getPropertyValue(loincCode, "parent"),
        // LOINC Parent Code
        getPropertyValue(loincCode, "child"),
        // LOINC Child Code
        getPropertyValue(loincCode, "CLASS"),
        // LOINC Class
        getPropertyValue(loincCode, "EXAMPLE_UCUM_UNITS"),
        // LOINC Example UCUM Units
        getPropertyValue(loincCode, "ORDER_OBS"),
        // LOINC Order / Observation Indicator
        getPropertyValue(loincCode, "CONSUMER_NAME")
        // LOINC Consumer Name
    );
    for (int i = 0; i < values.size(); i++) {
      insertStatement.setString(i + 1, values.get(i));
    }
    return values;
  }

  private boolean execute(String sql) throws SQLException {
    long start = System.nanoTime();
    Statement statement = connection.createStatement();
    boolean result = statement.execute(sql);
    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.debug("Executed statement in " + String.format("%.1f", elapsedMs) + " ms: " + sql);
    return result;
  }

  private boolean execute(PreparedStatement preparedStatement) throws SQLException {
    long start = System.nanoTime();
    boolean result = preparedStatement.execute();
    double elapsedMs = TimeUnit.MILLISECONDS
        .convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    logger.debug("Executed prepared statement in " + String.format("%.1f", elapsedMs) + " ms");
    return result;
  }

  private String getPropertyValue(LoincCode loincCode, String property) {
    if (loincCode.getLookupResponse() == null) {
      return null;
    }
    Predicate<Parameters.ParametersParameterComponent> partMatchesProperty =
        part -> part.getName().equals("code") && ((CodeType) part.getValue()).asStringValue()
            .equals(property);
    Predicate<Parameters.ParametersParameterComponent> paramIsProperty =
        parameter -> parameter.getName().equals("property");
    Predicate<Parameters.ParametersParameterComponent> paramContainsMatchingPart =
        parameter -> parameter.getPart().stream().anyMatch(partMatchesProperty);
    Predicate<Parameters.ParametersParameterComponent> valuePart = part -> part.getName()
        .startsWith("value");
    try {
      // Get the right parameter.
      Parameters.ParametersParameterComponent matchingParam = loincCode.getLookupResponse()
          .getParameter()
          .stream()
          .filter(paramIsProperty)
          .filter(paramContainsMatchingPart)
          .findFirst()
          .get();
      // Get the value of the parameter.
      Type value = matchingParam.getPart()
          .stream()
          .filter(valuePart)
          .findFirst()
          .get()
          .getValue();
      return value == null ? null : value.toString();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  private String getDesignationValue(LoincCode loincCode, String code) {
    if (loincCode.getLookupResponse() == null) {
      return null;
    }
    Predicate<Parameters.ParametersParameterComponent> partMatchesCode =
        part -> part.getName().equals("use") && ((Coding) part.getValue()).getCode().equals(code);
    Predicate<Parameters.ParametersParameterComponent> paramIsDesignation =
        parameter -> parameter.getName().equals("designation");
    Predicate<Parameters.ParametersParameterComponent> paramContainsMatchingCode =
        parameter -> parameter.getPart().stream().anyMatch(partMatchesCode);
    Predicate<Parameters.ParametersParameterComponent> valuePart = part -> part.getName()
        .startsWith("value");
    try {
      // Get the right parameter.
      Parameters.ParametersParameterComponent matchingParam = loincCode.getLookupResponse()
          .getParameter()
          .stream()
          .filter(paramIsDesignation)
          .filter(paramContainsMatchingCode)
          .findFirst()
          .get();
      // Get the value of the parameter.
      Type value = matchingParam.getPart()
          .stream()
          .filter(valuePart)
          .findFirst()
          .get()
          .getValue();
      return value == null ? null : value.toString();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

}
