package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class JoinFunctionDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testJoinFunction() {
    return builder()
        .withSubject(sb -> sb
            // Empty collections
            .string("emptyCollection", null)
            
            // Single item collections
            .stringArray("singleString", "hello")
            
            // Multiple item collections
            .stringArray("strings", "apple", "banana", "cherry")
            .stringArray("numbers", "1", "2", "3", "4", "5")
            .stringArray("mixed", "a", "1", "b", "2")
            
            // Collections with empty strings
            .stringArray("withEmpty", "first", "", "third")
            .stringArray("allEmpty", "", "", "")
            
            // Collections with special characters
            .stringArray("specialChars", "comma,here", "quote\"here", "newline\nhere")
            
            // Non-string collections (should be converted to strings)
            .integerArray("intNumbers", 1, 2, 3, 4, 5)
            .boolArray("booleans", true, false, true)
            .decimalArray("decimals", 1.1, 2.2, 3.3)
            
            // Nested collections for path expressions
            .complex("patient", p -> p
                .stringArray("names", "John", "Doe")
                .complexArray("telecom", 
                    t -> t.string("system", "phone").string("value", "555-1234"),
                    t -> t.string("system", "email").string("value", "john@example.com")
                )
                .complexArray("identifiers",
                    i -> i.string("system", "MRN").string("value", "12345"),
                    i -> i.string("system", "SSN").string("value", "123-45-6789")
                )
            )
            
            // For testing with expressions
            .complex("observation", o -> o
                .complexArray("component",
                    c -> c.string("code", "8480-6").decimal("value", 120.0),
                    c -> c.string("code", "8462-4").decimal("value", 80.0)
                )
            )
        )
        .group("Basic join() behavior")
        .testEmpty("emptyCollection.join()", "join() on empty collection returns empty")
        .testEquals("hello", "singleString.join()", "join() on single string returns the string")
        .testEquals("applebananacherry", "strings.join()", 
            "join() without separator concatenates strings")
        .testEquals("12345", "numbers.join()", 
            "join() without separator concatenates numeric strings")
        
        .group("Join with separator")
        .testEquals("apple,banana,cherry", "strings.join(',')", 
            "join() with comma separator")
        .testEquals("apple|banana|cherry", "strings.join('|')", 
            "join() with pipe separator")
        .testEquals("apple banana cherry", "strings.join(' ')", 
            "join() with space separator")
        .testEquals("apple-banana-cherry", "strings.join('-')", 
            "join() with hyphen separator")
        .testEquals("apple::banana::cherry", "strings.join('::')", 
            "join() with multi-character separator")
        .testEquals("apple\nbanana\ncherry", "strings.join('\n')", 
            "join() with newline separator")
        
        .group("Empty strings and separators")
        .testEquals("first,,third", "withEmpty.join(',')", 
            "join() preserves empty strings with separator")
        .testEquals("firstthird", "withEmpty.join()", 
            "join() without separator skips empty strings")
        .testEquals(",,", "allEmpty.join(',')", 
            "join() with all empty strings and separator")
        .testEquals("", "allEmpty.join()", 
            "join() with all empty strings and no separator")
        .testEquals("apple,,banana,,cherry", "strings.join(',,')", 
            "join() with multi-character separator containing empty parts")
        .testEquals("applebananacherry", "strings.join('')", 
            "join() with empty string separator is same as no separator")
        
        .group("Special characters")
        .testEquals("comma,here;quote\"here;newline\nhere", "specialChars.join(';')", 
            "join() with strings containing special characters")
        .testEquals("comma,here\"quote\"here\"newline\nhere", "specialChars.join('\"')", 
            "join() with quote as separator")
        
        .group("Non-string collections")
        .testEquals("12345", "intNumbers.join()", 
            "join() converts integers to strings")
        .testEquals("1,2,3,4,5", "intNumbers.join(',')", 
            "join() converts integers to strings with separator")
        .testEquals("truefalsetrue", "booleans.join()", 
            "join() converts booleans to strings")
        .testEquals("true,false,true", "booleans.join(',')", 
            "join() converts booleans to strings with separator")
        .testEquals("1.12.23.3", "decimals.join()", 
            "join() converts decimals to strings")
        .testEquals("1.1;2.2;3.3", "decimals.join(';')", 
            "join() converts decimals to strings with separator")
        
        .group("Path expressions")
        .testEquals("JohnDoe", "patient.names.join()", 
            "join() on path expression without separator")
        .testEquals("John,Doe", "patient.names.join(',')", 
            "join() on path expression with separator")
        .testEquals("555-1234,john@example.com", "patient.telecom.value.join(',')", 
            "join() on nested path expression")
        .testEquals("MRN:12345,SSN:123-45-6789", 
            "patient.identifiers.select(system & ':' & value).join(',')", 
            "join() with complex select expression")
        
        .group("With filtering and mapping")
        .testEquals("apple,cherry", "strings.where($this.startsWith('a') or $this.startsWith('c')).join(',')", 
            "join() after where() filter")
        .testEquals("APPLE,BANANA,CHERRY", "strings.select($this.upper()).join(',')", 
            "join() after select() transformation")
        .testEquals("5,6,7", "intNumbers.select($this + 4).join(',')", 
            "join() after numeric transformation")
        
        .group("Complex expressions")
        .testEquals("8480-6:120.0,8462-4:80.0", 
            "observation.component.select(code & ':' & value).join(',')", 
            "join() with complex select and concatenation")
        .testEquals("120.0,80.0", 
            "observation.component.where(code.exists()).select(value).join(',')", 
            "join() after where() and select()")
        
        .group("Error cases")
        .testError("patient.join(',')", 
            "join() on non-collection should error")
        
        .build();
  }
}
