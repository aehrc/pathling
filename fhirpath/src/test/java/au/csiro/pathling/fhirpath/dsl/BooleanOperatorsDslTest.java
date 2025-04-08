package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class BooleanOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testAndOperator() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("emptyBool", new Boolean[0])
        )
        .group("AND operator with literals")
        .testTrue("true and true", "true AND true = true")
        .testFalse("true and false", "true AND false = false")
        .testFalse("false and true", "false AND true = false")
        .testFalse("false and false", "false AND false = false")
        
        .group("AND operator with variables")
        .testTrue("trueValue and trueValue", "trueValue AND trueValue = true")
        .testFalse("trueValue and falseValue", "trueValue AND falseValue = false")
        .testFalse("falseValue and trueValue", "falseValue AND trueValue = false")
        .testFalse("falseValue and falseValue", "falseValue AND falseValue = false")
        
        .group("AND operator with empty collections")
        .testEmpty("trueValue and {}", "true AND empty = empty")
        .testFalse("falseValue and {}", "false AND empty = false")
        .testEmpty("{} and trueValue", "empty AND true = empty")
        .testFalse("{} and falseValue", "empty AND false = false")
        .testEmpty("{} and {}", "empty AND empty = empty")
        .testEmpty("trueValue and emptyBool", "true AND empty bool = empty")
        .testFalse("falseValue and emptyBool", "false AND empty bool = false")
        
        .group("AND operator with expressions")
        .testTrue("(1 < 2) and (3 > 2)", "true expression AND true expression = true")
        .testFalse("(1 < 2) and (3 < 2)", "true expression AND false expression = false")
        .testFalse("(1 > 2) and (3 > 2)", "false expression AND true expression = false")
        .testFalse("(1 > 2) and (3 < 2)", "false expression AND false expression = false")
        .testEmpty("(1 < 2) and ({})", "true expression AND empty = empty")
        .testFalse("(1 > 2) and ({})", "false expression AND empty = false")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testOrOperator() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("emptyBool", new Boolean[0])
        )
        .group("OR operator with literals")
        .testTrue("true or true", "true OR true = true")
        .testTrue("true or false", "true OR false = true")
        .testTrue("false or true", "false OR true = true")
        .testFalse("false or false", "false OR false = false")
        
        .group("OR operator with variables")
        .testTrue("trueValue or trueValue", "trueValue OR trueValue = true")
        .testTrue("trueValue or falseValue", "trueValue OR falseValue = true")
        .testTrue("falseValue or trueValue", "falseValue OR trueValue = true")
        .testFalse("falseValue or falseValue", "falseValue OR falseValue = false")
        
        .group("OR operator with empty collections")
        .testTrue("trueValue or {}", "true OR empty = true")
        .testEmpty("falseValue or {}", "false OR empty = empty")
        .testTrue("{} or trueValue", "empty OR true = true")
        .testEmpty("{} or falseValue", "empty OR false = empty")
        .testEmpty("{} or {}", "empty OR empty = empty")
        .testTrue("trueValue or emptyBool", "true OR empty bool = true")
        .testEmpty("falseValue or emptyBool", "false OR empty bool = empty")
        
        .group("OR operator with expressions")
        .testTrue("(1 < 2) or (3 > 2)", "true expression OR true expression = true")
        .testTrue("(1 < 2) or (3 < 2)", "true expression OR false expression = true")
        .testTrue("(1 > 2) or (3 > 2)", "false expression OR true expression = true")
        .testFalse("(1 > 2) or (3 < 2)", "false expression OR false expression = false")
        .testTrue("(1 < 2) or ({})", "true expression OR empty = true")
        .testEmpty("(1 > 2) or ({})", "false expression OR empty = empty")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testXorOperator() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("emptyBool", new Boolean[0])
        )
        .group("XOR operator with literals")
        .testFalse("true xor true", "true XOR true = false")
        .testTrue("true xor false", "true XOR false = true")
        .testTrue("false xor true", "false XOR true = true")
        .testFalse("false xor false", "false XOR false = false")
        
        .group("XOR operator with variables")
        .testFalse("trueValue xor trueValue", "trueValue XOR trueValue = false")
        .testTrue("trueValue xor falseValue", "trueValue XOR falseValue = true")
        .testTrue("falseValue xor trueValue", "falseValue XOR trueValue = true")
        .testFalse("falseValue xor falseValue", "falseValue XOR falseValue = false")
        
        .group("XOR operator with empty collections")
        .testEmpty("trueValue xor {}", "true XOR empty = empty")
        .testEmpty("falseValue xor {}", "false XOR empty = empty")
        .testEmpty("{} xor trueValue", "empty XOR true = empty")
        .testEmpty("{} xor falseValue", "empty XOR false = empty")
        .testEmpty("{} xor {}", "empty XOR empty = empty")
        .testEmpty("trueValue xor emptyBool", "true XOR empty bool = empty")
        .testEmpty("falseValue xor emptyBool", "false XOR empty bool = empty")
        
        .group("XOR operator with expressions")
        .testFalse("(1 < 2) xor (3 > 2)", "true expression XOR true expression = false")
        .testTrue("(1 < 2) xor (3 < 2)", "true expression XOR false expression = true")
        .testTrue("(1 > 2) xor (3 > 2)", "false expression XOR true expression = true")
        .testFalse("(1 > 2) xor (3 < 2)", "false expression XOR false expression = false")
        .testEmpty("(1 < 2) xor ({})", "true expression XOR empty = empty")
        .testEmpty("(1 > 2) xor ({})", "false expression XOR empty = empty")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testImpliesOperator() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("emptyBool", new Boolean[0])
        )
        .group("IMPLIES operator with literals")
        .testTrue("true implies true", "true IMPLIES true = true")
        .testFalse("true implies false", "true IMPLIES false = false")
        .testTrue("false implies true", "false IMPLIES true = true")
        .testTrue("false implies false", "false IMPLIES false = true")
        
        .group("IMPLIES operator with variables")
        .testTrue("trueValue implies trueValue", "trueValue IMPLIES trueValue = true")
        .testFalse("trueValue implies falseValue", "trueValue IMPLIES falseValue = false")
        .testTrue("falseValue implies trueValue", "falseValue IMPLIES trueValue = true")
        .testTrue("falseValue implies falseValue", "falseValue IMPLIES falseValue = true")
        
        .group("IMPLIES operator with empty collections")
        .testEmpty("trueValue implies {}", "true IMPLIES empty = empty")
        .testTrue("falseValue implies {}", "false IMPLIES empty = true")
        .testTrue("{} implies trueValue", "empty IMPLIES true = true")
        .testEmpty("{} implies falseValue", "empty IMPLIES false = empty")
        .testEmpty("{} implies {}", "empty IMPLIES empty = empty")
        .testEmpty("trueValue implies emptyBool", "true IMPLIES empty bool = empty")
        .testTrue("falseValue implies emptyBool", "false IMPLIES empty bool = true")
        
        .group("IMPLIES operator with expressions")
        .testTrue("(1 < 2) implies (3 > 2)", "true expression IMPLIES true expression = true")
        .testFalse("(1 < 2) implies (3 < 2)", "true expression IMPLIES false expression = false")
        .testTrue("(1 > 2) implies (3 > 2)", "false expression IMPLIES true expression = true")
        .testTrue("(1 > 2) implies (3 < 2)", "false expression IMPLIES false expression = true")
        .testEmpty("(1 < 2) implies ({})", "true expression IMPLIES empty = empty")
        .testTrue("(1 > 2) implies ({})", "false expression IMPLIES empty = true")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testNotFunction() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("emptyBool", new Boolean[0])
        )
        .group("NOT function with literals")
        .testFalse("true.not()", "NOT true = false")
        .testTrue("false.not()", "NOT false = true")
        
        .group("NOT function with variables")
        .testFalse("trueValue.not()", "NOT trueValue = false")
        .testTrue("falseValue.not()", "NOT falseValue = true")
        
        .group("NOT function with empty collections")
        .testEmpty("{}.not()", "NOT empty = empty")
        .testEmpty("emptyBool.not()", "NOT empty bool = empty")
        
        .group("NOT function with expressions")
        .testFalse("(1 < 2).not()", "NOT true expression = false")
        .testTrue("(1 > 2).not()", "NOT false expression = true")
        .testEmpty("({}).not()", "NOT empty expression = empty")
        
        .group("NOT function with complex expressions")
        .testTrue("(true and false).not()", "NOT (true AND false) = true")
        .testFalse("(true or false).not()", "NOT (true OR false) = false")
        .testTrue("(true xor true).not()", "NOT (true XOR true) = true")
        .testFalse("(true implies true).not()", "NOT (true IMPLIES true) = false")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testComplexBooleanExpressions() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .integer("age", 25)
            .string("status", "active")
            .complex("patient", p -> p
                .bool("active", true)
                .integer("age", 30)
                .string("gender", "male")
            )
        )
        .group("Complex boolean expressions")
        .testTrue("true and (false or true)", "true AND (false OR true) = true")
        .testFalse("(true and false) or (false and false)", "(true AND false) OR (false AND false) = false")
        .testTrue("(true and true) or (false and true)", "(true AND true) OR (false AND true) = true")
        .testTrue("(false implies true) and true", "(false IMPLIES true) AND true = true")
        .testFalse("true xor (true xor false)", "true XOR (true XOR false) = false")
        .testTrue("(true xor true) xor false", "(true XOR true) XOR false = false")
        
        .group("Boolean expressions with comparisons")
        .testTrue("(age > 20) and (status = 'active')", "(age > 20) AND (status = 'active') = true")
        .testFalse("(age < 20) and (status = 'active')", "(age < 20) AND (status = 'active') = false")
        .testTrue("(age < 20) or (status = 'active')", "(age < 20) OR (status = 'active') = true")
        .testTrue("(patient.active = true) and (patient.gender = 'male')", 
            "(patient.active = true) AND (patient.gender = 'male') = true")
        .testFalse("(patient.age < 25) and (patient.gender = 'female')", 
            "(patient.age < 25) AND (patient.gender = 'female') = false")
        .testTrue("(patient.age > 25) or (patient.gender = 'female')", 
            "(patient.age > 25) OR (patient.gender = 'female') = true")
        
        .group("Boolean expressions with empty collections")
        .testEmpty("(age > 20) and (missing = true)", "(age > 20) AND (missing = true) = empty")
        .testFalse("(age < 20) and (missing = true)", "(age < 20) AND (missing = true) = false")
        .testTrue("(age > 20) or (missing = true)", "(age > 20) OR (missing = true) = true")
        .testEmpty("(age < 20) or (missing = true)", "(age < 20) OR (missing = true) = empty")
        
        .build();
  }
}
