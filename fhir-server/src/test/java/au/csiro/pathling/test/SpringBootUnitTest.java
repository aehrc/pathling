package au.csiro.pathling.test;


import au.csiro.pathling.UnitTestDependencies;
import org.junit.jupiter.api.Tag;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = UnitTestDependencies.class)
@ActiveProfiles({"unit-test"})
@Tag("UnitTest")
public @interface SpringBootUnitTest {

}
