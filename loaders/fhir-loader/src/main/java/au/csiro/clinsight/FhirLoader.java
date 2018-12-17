/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import au.csiro.clinsight.persistence.Dimension;
import au.csiro.clinsight.persistence.DimensionAttribute;
import au.csiro.clinsight.persistence.FactSet;
import au.csiro.clinsight.persistence.Metric;
import ca.uhn.fhir.context.FhirContext;
import org.apache.commons.io.IOUtils;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static au.csiro.clinsight.FileReferences.urlToStream;

/**
 * Takes either a FHIR transaction or an NDJSON file as input, and updates the target data mart with the information
 * within the FHIR resource(s).
 *
 * @author John Grimes
 */
public class FhirLoader {

    private static final Logger logger = LoggerFactory.getLogger(FhirLoader.class);
    private final FhirContext fhirContext;
    private String jdbcUrl;
    private String terminologyServerUrl;
    private String jdbcDriver;
    private String autoDdl;
    private SessionFactory sessionFactory;

    public FhirLoader(String jdbcUrl, String terminologyServerUrl, String jdbcDriver, String autoDdl) {
        this.jdbcUrl = jdbcUrl;
        this.terminologyServerUrl = terminologyServerUrl;
        this.jdbcDriver = jdbcDriver;
        this.autoDdl = autoDdl;
        fhirContext = initialiseFhirContext();
        sessionFactory = initialiseSessionFactory();
    }

    private static Bundle validateAsTransaction(IBaseResource resource) throws Exception {
        Bundle fhirTransaction = (Bundle) resource;
        String resourceType = fhirTransaction.getResourceType().toString();
        String bundleType = fhirTransaction.getType().toString();
        if (!resourceType.equals("Bundle") || !bundleType.equals("TRANSACTION")) {
            throw new Exception("Input resource encountered that is not FHIR transaction");
        }
        return fhirTransaction;
    }

    /**
     * Loads the content of a single transaction (Bundle), accessible from the supplied URL.
     */
    public void processTransaction(String transactionUrl) throws Exception {
        long start = System.nanoTime();
        TransactionLoader transactionLoader = new TransactionLoader(fhirContext,
                                                                    sessionFactory,
                                                                    jdbcUrl,
                                                                    terminologyServerUrl);
        InputStream inputStream = urlToStream(new URL(transactionUrl));
        String json = IOUtils.toString(inputStream, "UTF-8");
        IBaseResource resource = fhirContext.newJsonParser().parseResource(json);
        Bundle fhirTransaction = validateAsTransaction(resource);
        transactionLoader.execute(fhirTransaction);
        double elapsedMilliseconds = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start,
                                                                   TimeUnit.NANOSECONDS);
        logger.info("Processed transaction (" + transactionUrl + ") in " +
                            String.format("%.1f", elapsedMilliseconds) + " ms");
    }

    /**
     * Loads each transaction (Bundle) within an NDJSON file, accessible from the supplied URL.
     */
    public void processNdjsonFile(String ndjsonUrl) throws Exception {
        long allStart = System.nanoTime();
        TransactionLoader transactionLoader = new TransactionLoader(fhirContext,
                                                                    sessionFactory,
                                                                    jdbcUrl,
                                                                    terminologyServerUrl);
        InputStream inputStream = urlToStream(new URL(ndjsonUrl));
        NdjsonFile ndjson = new NdjsonFile(fhirContext, inputStream);
        for (IBaseResource resource : ndjson) {
            long start = System.nanoTime();
            Bundle fhirTransaction = validateAsTransaction(resource);
            transactionLoader.execute(fhirTransaction);
            double elapsedMilliseconds = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start,
                                                                       TimeUnit.NANOSECONDS);
            logger.info("[" + ndjson.getCurrentIndex() + "] Processed transaction in " +
                                String.format("%.1f", elapsedMilliseconds) + " ms");
        }
        double allElapsedSeconds = TimeUnit.SECONDS.convert(System.nanoTime() - allStart,
                                                            TimeUnit.NANOSECONDS);
        logger.info("Processed all transactions in NDJSON file (" + ndjsonUrl + ") in " +
                            String.format("%.1f", allElapsedSeconds) + " secs");
    }

    private FhirContext initialiseFhirContext() {
        FhirContext fhirContext;
        fhirContext = FhirContext.forDstu3();

        // Declare custom types.
        List<Class<? extends IBase>> customTypes = new ArrayList<>();
        customTypes.add(Dimension.class);
        customTypes.add(DimensionAttribute.class);
        customTypes.add(Metric.class);

        fhirContext.registerCustomTypes(customTypes);
        return fhirContext;
    }

    private SessionFactory initialiseSessionFactory() {
        Properties properties = new Properties();
        properties.setProperty("hibernate.connection.driver_class", jdbcDriver);
        properties.setProperty("hibernate.hbm2ddl.auto", autoDdl);
        properties.setProperty("hibernate.connection.url", jdbcUrl);
        Configuration configuration = new Configuration()
                .addAnnotatedClass(Dimension.class)
                .addAnnotatedClass(Dimension.DescribesComponent.class)
                .addAnnotatedClass(Metric.class)
                .addAnnotatedClass(FactSet.class)
                .setProperties(properties);
        return configuration.buildSessionFactory();
    }

}
