/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import au.csiro.clinsight.transformers.EpcarToFhirTransformer;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import org.apache.commons.io.IOUtils;
import org.hl7.fhir.dstu3.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.skyscreamer.jsonassert.JSONAssert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class EpcarToFhirTransformerTest {

    private static final Logger logger = LoggerFactory.getLogger(EpcarToFhirTransformerTest.class);
    private HapiContext hapiContext;
    private FhirContext fhirContext;
    private String v2Message;
    private String fhirBundle;
    private InputStream v2MessageStream;
    private InputStream fhirBundleStream;
    private Parameters params1;

    @Mock
    private TerminologyClient terminologyClient;

    @Before
    public void setUp() throws Exception {
        hapiContext = new DefaultHapiContext();
        fhirContext = FhirContext.forDstu3();
        v2MessageStream = getClass().getResourceAsStream("/v2/1.1631610836532992.29761628");
        fhirBundleStream = getClass().getResourceAsStream("/fhir/1.1631610836532992.29761628.Bundle.json");
        v2Message = IOUtils.toString(v2MessageStream, StandardCharsets.UTF_8);
        fhirBundle = IOUtils.toString(fhirBundleStream, StandardCharsets.UTF_8);

        // Set up an input Parameters object.
        params1 = new Parameters();

        // The `result` parameter.
        Parameters.ParametersParameterComponent result = new Parameters.ParametersParameterComponent();
        result.setName("result");
        result.setValue(new BooleanType(true));
        params1.addParameter(result);

        // The first `match` parameter.
        Parameters.ParametersParameterComponent match1 = new Parameters.ParametersParameterComponent();
        match1.setName("match");
        Parameters.ParametersParameterComponent equivalence1 = new Parameters.ParametersParameterComponent();
        equivalence1.setName("equivalence");
        equivalence1.setValue(new StringType("narrower"));
        Parameters.ParametersParameterComponent concept1 = new Parameters.ParametersParameterComponent();
        Coding coding1 = new Coding();
        coding1.setCode("FOO");
        coding1.setSystem("http://somedomain.com/fhir/foobar");
        concept1.setName("concept");
        concept1.setValue(coding1);
        List<Parameters.ParametersParameterComponent> matchParts1 = Arrays.asList(equivalence1, concept1);
        match1.setPart(matchParts1);
        params1.addParameter(match1);

        // The second `match` parameter.
        Parameters.ParametersParameterComponent match2 = new Parameters.ParametersParameterComponent();
        match2.setName("match");
        Parameters.ParametersParameterComponent equivalence2 = new Parameters.ParametersParameterComponent();
        equivalence2.setName("equivalence");
        equivalence2.setValue(new StringType("equivalent"));
        Parameters.ParametersParameterComponent concept2 = new Parameters.ParametersParameterComponent();
        Coding coding2 = new Coding();
        coding2.setCode("BAR");
        coding2.setSystem("http://somedomain.com/fhir/foobar");
        concept2.setName("concept");
        concept2.setValue(coding2);
        List<Parameters.ParametersParameterComponent> matchParts2 = Arrays.asList(equivalence2, concept2);
        match2.setPart(matchParts2);
        params1.addParameter(match2);
    }

    @Test
    public void transform() throws Exception {
        Mockito.when(terminologyClient.translate(ArgumentMatchers.any(),
                                                 ArgumentMatchers.any(),
                                                 ArgumentMatchers.any(),
                                                 ArgumentMatchers
                                                         .any())).thenReturn(params1);
        EpcarToFhirTransformer transformer = new EpcarToFhirTransformer(hapiContext,
                                                                        terminologyClient);
        Bundle result = transformer.transform(v2Message);
        String resultJson = fhirContext.newJsonParser().encodeResourceToString(result);
        logger.debug("Result: " + resultJson);
        JSONAssert.assertEquals(fhirBundle, resultJson, true);
    }

    @After
    public void tearDown() throws Exception {
        v2MessageStream.close();
        fhirBundleStream.close();
    }

}
