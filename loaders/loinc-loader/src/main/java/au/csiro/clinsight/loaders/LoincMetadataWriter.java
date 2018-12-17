/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.loaders;

import au.csiro.clinsight.persistence.Dimension;
import au.csiro.clinsight.persistence.DimensionAttribute;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LoincMetadataWriter {

    private static final Logger logger = LoggerFactory.getLogger(LoincMetadataWriter.class

    );
    private IParser jsonParser;
    private SessionFactory sessionFactory;
    private String url;
    private String version;
    private Dimension dimension;
    private List<DimensionAttribute> dimensionAttributes;

    public LoincMetadataWriter(FhirContext fhirContext, SessionFactory sessionFactory, String url, String version) {
        this.sessionFactory = sessionFactory;
        this.url = url;
        this.version = version;
        jsonParser = fhirContext.newJsonParser();
        dimensionAttributes = new ArrayList<>();
        create();
    }

    private void create() {
        Dimension.DescribesCodeSystemComponent codeSystem = new Dimension.DescribesCodeSystemComponent();
        codeSystem.setUrl(url);
        codeSystem.setVersion(version);

        Dimension.DescribesComponent describes = new Dimension.DescribesComponent();
        describes.setCodeSystem(codeSystem);

        dimension = new Dimension();
        dimension.setName("LOINC");
        dimension.setTitle("loinc");
        dimension.getDescribes().add(describes);
        dimension.setAttribute(new ArrayList<>());

        createDimensionAttribute("Code", "code", "string");
        createDimensionAttribute("Long Name", "longName", "string");
        createDimensionAttribute("Fully Specified Name", "fsn", "string");
        createDimensionAttribute("Code Status", "status", "string");
        createDimensionAttribute("Component", "component", "string");
        createDimensionAttribute("Method", "method", "string");
        createDimensionAttribute("Property", "property", "string");
        createDimensionAttribute("Scale", "scale", "string");
        createDimensionAttribute("System", "system", "string");
        createDimensionAttribute("Time Aspect", "timeAspect", "string");
        createDimensionAttribute("Code Category", "category", "string");
        createDimensionAttribute("Answer List Code", "answerList", "string");
        createDimensionAttribute("Parent Code", "parent", "string");
        createDimensionAttribute("Child Code", "child", "string");
        createDimensionAttribute("Class", "class", "string");
        createDimensionAttribute("Example UCUM Units", "exampleUcum", "string");
        createDimensionAttribute("Order / Observation Indicator", "orderObservation", "string");
        createDimensionAttribute("Consumer-Friendly Name", "consumerFriendlyName", "string");

        dimension.generateJson(jsonParser);
    }

    public Dimension getDimension() {
        return dimension;
    }

    public List<DimensionAttribute> getDimensionAttributes() {
        return dimensionAttributes;
    }

    public void write() {
        Session session = sessionFactory.openSession();
        try {
            Transaction transaction = session.beginTransaction();
            session.save(dimension);
            logger.info("Wrote new Dimension to store: " + dimension.getKey());
            for (DimensionAttribute dimensionAttribute : dimensionAttributes) {
                session.save(dimensionAttribute);
                logger.info("Wrote new DimensionAttribute to store: " + dimensionAttribute.getKey());
            }
            transaction.commit();
        } finally {
            session.close();
        }
    }

    private void createDimensionAttribute(String name, String title, String typeCode) {
        DimensionAttribute dimensionAttribute = new DimensionAttribute();
        dimensionAttribute.setName(name);
        dimensionAttribute.setTitle(title);
        Coding dimensionAttributeType = new Coding();
        dimensionAttributeType.setSystem("http://hl7.org/fhir/data-types");
        dimensionAttributeType.setCode(typeCode);
        dimensionAttributeType.setDisplay("String");
        dimensionAttribute.setType(dimensionAttributeType);

        Reference dimensionReference = new Reference();
        dimensionReference.setReference("Dimension/" + dimension.getIdElement().getIdPart());
        dimensionAttribute.setDimension(dimensionReference);

        dimensionAttribute.generateJson(jsonParser);
        dimensionAttributes.add(dimensionAttribute);

        Reference dimensionAttributeReference = new Reference();
        dimensionAttributeReference.setReference("DimensionAttribute/" + dimensionAttribute.getIdElement().getIdPart());
        dimensionAttributeReference.setDisplay(dimensionAttribute.getName());
        dimension.getAttribute().add(dimensionAttributeReference);
    }

}
