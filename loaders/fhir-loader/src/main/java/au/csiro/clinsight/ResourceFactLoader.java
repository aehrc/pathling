/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import au.csiro.clinsight.persistence.Dimension;
import au.csiro.clinsight.persistence.FactSet;
import au.csiro.clinsight.persistence.Metric;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hibernate.Session;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.exceptions.FHIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.TypedQuery;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static au.csiro.clinsight.persistence.Naming.*;

public class ResourceFactLoader {

    private static final Logger logger = LoggerFactory.getLogger(FhirLoader.class);
    private IParser jsonParser;
    private Session session;
    private TerminologyClient terminologyClient;

    public ResourceFactLoader(FhirContext fhirContext, Session session, TerminologyClient terminologyClient) {
        this.session = session;
        this.terminologyClient = terminologyClient;
        jsonParser = fhirContext.newJsonParser();
    }

    public void execute(Resource resource, Bundle bundle) throws SQLException, FHIRException {
        FactSet factSet = ensureFactSetExists(resource);
        ensureMetricsExist(factSet);
        String factKey = insertFactRow(factSet, resource);
        for (Property property : resource.children()) {
            if (property.getTypeCode().equals("CodeableConcept")) {
                loadCodeableConcept(factSet, factKey, resource, property);
            }
        }
    }

    private FactSet ensureFactSetExists(Resource resource) throws SQLException {
        FactSet factSet;
        TypedQuery<FactSet> query = session.createQuery("SELECT f FROM FactSet f WHERE f.name = :name", FactSet.class);
        query.setParameter("name", resource.fhirType());
        Optional<FactSet> maybeFactSet = query.getResultStream().findFirst();
        if (maybeFactSet.isPresent()) {
            factSet = maybeFactSet.get();
        } else {
            factSet = new FactSet();
            factSet.setName(resource.fhirType());
            factSet.setTitle(lowerFirstLetter(resource.fhirType()));
            session.save(factSet);
        }
        String tableName = tableNameForFactSet(factSet);
        executeStatement("CREATE TABLE IF NOT EXISTS " + tableName + "(key text PRIMARY KEY)");
        return factSet;
    }

    // TODO: Add more metrics.
    private void ensureMetricsExist(FactSet factSet) {
        String name = "Number of " + pluraliseResourceName(factSet.getName());
        String title = "numberOf" + pluraliseResourceName(factSet.getTitle());
        TypedQuery<Metric> query = session.createQuery(
                "SELECT m FROM Metric m WHERE m.factSet.key = :factSetKey AND m.name = :name",
                Metric.class);
        query.setParameter("factSetKey", factSet.getKey());
        query.setParameter("name", name);
        Optional<Metric> maybeMetric = query.getResultStream().findFirst();
        if (!maybeMetric.isPresent()) {
            Metric metric = new Metric();
            metric.setFactSet(factSet);
            metric.setName(name);
            metric.setTitle(title);
            metric.setExpression("COUNT(*)");
            metric.generateJson(jsonParser);
            session.save(metric);
        }
    }

    private String insertFactRow(FactSet factSet, Resource resource) throws SQLException {
        String factTableName = tableNameForFactSet(factSet);
        String key = resource.getIdElement().getIdPart();
        PreparedStatement insert = prepareStatement(
                "INSERT INTO " + factTableName + " VALUES (?) ON CONFLICT DO NOTHING");
        insert.setString(1, key);
        executeStatement(insert);
        return key;
    }

    private void loadCodeableConcept(FactSet factSet, String factKey, Resource resource,
                                     Property property) throws SQLException, FHIRException {
        List<Base> values = property.getValues();
        Dimension valueSetDimension = getValueSetDimensionForCodeableConcept(resource, property);
        for (Base value : values) {
            CodeableConcept codeableConcept = (CodeableConcept) value;
            if (codeableConcept == null) {
                logger.debug("Null value encountered within CodeableConcept element");
                return;
            }
            for (Coding coding : codeableConcept.getCoding()) {
                String system = coding.getSystem();
                String code = coding.getCode();
                if (system == null || code == null) {
                    logger.debug("Coding encountered with non-null system and code");
                    return;
                }
                String version = coding.getVersion();
                Dimension codeSystemDimension = findCodeSystemDimension(system, version);
                if (codeSystemDimension != null) {
                    String dimensionKey = lookupKeyFromCodeSystemDimension(codeSystemDimension, code);
                    if (dimensionKey != null) linkMultiValuedDimension(factSet,
                                                                       factKey,
                                                                       codeSystemDimension,
                                                                       dimensionKey,
                                                                       property);
                }
                if (valueSetDimension != null) {
                    String valueSetKey = lookupKeyFromValueSetDimension(valueSetDimension, code);
                    if (valueSetKey != null) linkMultiValuedDimension(factSet,
                                                                      factKey,
                                                                      valueSetDimension,
                                                                      valueSetKey,
                                                                      property);
                }
            }
        }
    }

    private void linkMultiValuedDimension(FactSet factSet, String factKey, Dimension dimension,
                                          String dimensionKey, Property property)
            throws SQLException {
        String factTableName = tableNameForFactSet(factSet);
        String dimensionTableName = tableNameForDimension(dimension);
        String bridgeTableName = tableNameForMultiValuedDimension(factSet, property.getName(), dimension);
        executeStatement("CREATE TABLE IF NOT EXISTS " + bridgeTableName + " (" + factTableName + " text, " +
                                 dimensionTableName +
                                 " text, PRIMARY KEY (" + factTableName + ", " + dimensionTableName + "))");
        PreparedStatement insert = prepareStatement(
                "INSERT INTO " + bridgeTableName + " (" + factTableName + ", " + dimensionTableName +
                        ") VALUES (?, ?) ON CONFLICT DO NOTHING");
        insert.setString(1, factKey);
        insert.setString(2, dimensionKey);
        executeStatement(insert);
    }

    private Dimension getValueSetDimensionForCodeableConcept(Resource resource, Property property)
            throws FHIRException {
        ElementDefinition elementDefinition = terminologyClient.getElementDefinitionForProperty(resource, property);
        if (elementDefinition == null) return null;
        ElementDefinition.ElementDefinitionBindingComponent binding = elementDefinition.getBinding();
        String valueSetUrl = binding.getValueSetReference() == null
                             ? binding.getValueSetUriType().asStringValue()
                             : binding.getValueSetReference().getReference();
        return findValueSetDimension(valueSetUrl);
    }

    private Dimension findCodeSystemDimension(String system, String version) {
        String queryString =
                "SELECT d FROM Dimension d JOIN d.describes dd WHERE dd.codeSystem.url = :url";
        if (version != null) queryString += " WHERE dd.codeSystem.version = :version";
        TypedQuery<Dimension> query = session.createQuery(queryString, Dimension.class);
        query.setParameter("url", system);
        if (version != null) query.setParameter("version", version);
        Optional<Dimension> maybeDimension = query.getResultStream().findFirst();
        if (!maybeDimension.isPresent()) {
            String message = "Dimension not found matching encountered CodeSystem: " + system;
            if (version != null) message += ", " + version;
            logger.debug(message);
            return null;
        }
        Dimension dimension = maybeDimension.get();
        dimension.populateFromJson(jsonParser);
        return dimension;
    }

    private Dimension findValueSetDimension(String valueSet) {
        TypedQuery<Dimension> query = session.createQuery(
                "SELECT d FROM Dimension d JOIN d.describes dd WHERE dd.valueSet = :valueSet", Dimension.class);
        query.setParameter("valueSet", valueSet);
        Optional<Dimension> maybeDimension = query.getResultStream().findFirst();
        if (!maybeDimension.isPresent()) {
            String message = "Dimension not found matching ValueSet URL: " + valueSet;
            logger.debug(message);
            return null;
        }
        Dimension dimension = maybeDimension.get();
        dimension.populateFromJson(jsonParser);
        return dimension;
    }

    private String lookupKeyFromCodeSystemDimension(Dimension dimension, String code) throws SQLException {
        String tableName = tableNameForDimension(dimension);
        PreparedStatement statement = prepareStatement("SELECT key FROM " + tableName + " WHERE code = ?");
        statement.setString(1, code);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getString(1);
        } else {
            return null;
        }
    }

    private String lookupKeyFromValueSetDimension(Dimension dimension, String code) throws SQLException {
        String tableName = tableNameForDimension(dimension);
        PreparedStatement statement = prepareStatement("SELECT key FROM " + tableName + " WHERE code = ?");
        statement.setString(1, code);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getString(1);
        } else {
            return null;
        }
    }

    // private void ensureForeignKeyExistsInFactTable(Resource resource, Property property)
    //         throws IOException, SQLException {
    //     String tableName = tableNameForResourceFact(resource);
    //     PreparedStatement statement =
    //             connection.prepareStatement("ALTER TABLE ? ADD COLUMN IF NOT EXISTS ? text;");
    //     statement.setString(1, tableName);
    //     statement.setString(2, property.getName());
    //     executeStatement(statement);
    // }

    private PreparedStatement prepareStatement(String sql) {
        return session.doReturningWork(connection -> connection.prepareStatement(sql));
    }

    private boolean executeStatement(String sql) {
        return session.doReturningWork(connection -> {
            long start = System.nanoTime();
            Statement statement = connection.createStatement();
            boolean result = statement.execute(sql);
            double elapsedMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            logger.debug("Executed statement in " + String.format("%.1f", elapsedMs) + " ms: " + sql);
            return result;
        });
    }

    private boolean executeStatement(PreparedStatement preparedStatement) throws SQLException {
        return session.doReturningWork(connection -> {
            long start = System.nanoTime();
            boolean result = preparedStatement.execute();
            double elapsedMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            logger.debug("Executed prepared statement in " + String.format("%.1f", elapsedMs) + " ms");
            return result;
        });
    }

}
