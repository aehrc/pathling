/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.exceptions.FHIRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Updates the target data mart with the information within the supplied FHIR transaction.
 *
 * @author John Grimes
 */
public class TransactionLoader implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionLoader.class);
    private FhirContext fhirContext;
    private Session session;
    private TerminologyClient terminologyClient;
    private Set<ResourceType> factCandidates = new HashSet<>(Arrays.asList(
            new ResourceType("Observation", "http://hl7.org/fhir/Profile/Observation")
    ));

    public TransactionLoader(FhirContext fhirContext, SessionFactory sessionFactory, String jdbcUrl,
                             String terminologyServerEndpoint) {
        this.fhirContext = fhirContext;
        session = sessionFactory.openSession();
        terminologyClient = new TerminologyClient(fhirContext, terminologyServerEndpoint);
    }

    public void execute(Bundle fhirTransaction) throws SQLException, FHIRException {
        Transaction transaction = session.beginTransaction();
        try {
            ResourceFactLoader resourceFactLoader = new ResourceFactLoader(fhirContext,
                                                                           session,
                                                                           terminologyClient);
            List<Bundle.BundleEntryComponent> entries = fhirTransaction.getEntry()
                                                                       .stream()
                                                                       .filter(entryTypeIsFactCandidate())
                                                                       .collect(Collectors.toList());
            for (Bundle.BundleEntryComponent entry : entries) {
                Resource resource = entry.getResource();
                resourceFactLoader.execute(resource, fhirTransaction);
            }
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            throw e;
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
        }
    }

    private Predicate<? super Bundle.BundleEntryComponent> entryTypeIsFactCandidate() {
        return entry -> factCandidates.stream()
                                      .anyMatch(factCandidate -> entry.getResource()
                                                                      .getResourceType()
                                                                      .toString()
                                                                      .equals(factCandidate.getResourceName()));
    }

    public void close() {
        session.close();
    }

}
