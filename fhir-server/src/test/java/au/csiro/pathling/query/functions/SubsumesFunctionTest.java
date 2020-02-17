/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.getSparkSession;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalCoding;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import au.csiro.pathling.bunsen.FhirEncoders;
import au.csiro.pathling.encoding.SystemAndCode;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.CodingRowFixture;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class SubsumesFunctionTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = getSparkSession();
  }

  @Test
  public void testGetsFirstResourceCorrectly() {
    
    
    List<SystemAndCode> codings = Collections.singletonList(new SystemAndCode());
    Dataset<SystemAndCode> argumentDataset = spark.createDataset(codings, Encoders.bean(SystemAndCode.class));
    //List<Coding> codings = Collections.singletonList(new Coding());
    //Dataset<CodingBean> argumentDataset = spark.createDataset(codings, FhirEncoders.forR4().getOrCreate().of(Coding.class));
    
    argumentDataset.show();
    
//    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.CODING, CODING)
//        .withDataset(CodingRowFixture.createCompleteDataset(spark))
//        .build();
//    ParsedExpression element = literalCoding(CodingRowFixture.SYSTEM_2, CodingRowFixture.VERSION_2,
//        CodingRowFixture.CODE_2);
//    
//  
//    FunctionInput firstInput = new FunctionInput();
//    firstInput.setInput(collection);
//    firstInput.getArguments().add(element);
//    firstInput.setExpression("subsumes()");
//    firstInput.setContext(new ExpressionParserContext());
//
//    // Execute the first function.
//    Function firstFunction = new SubsumesFunction();
//    
//    ParsedExpression result = firstFunction.invoke(firstInput);
//    assertThat(result).selectResult().debugAllRows();
  }
  
}
