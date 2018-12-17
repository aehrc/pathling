package au.csiro.clinsight.persistence;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * @author John Grimes
 */
public class NamingTest {

    private Dimension dimension;
    private FactSet factSet;

    @Before
    public void setUp() {
        dimension = new Dimension();
        dimension.setKey("cf2d4289");
        dimension.setName("ProcedureCategory");
        dimension.setTitle("Procedure Category");

        factSet = new FactSet();
        factSet.setKey("f6bcea2b");
        factSet.setName("ProcedureRequest");
    }

    @Test
    public void testTableNameForDimension() {
        String result = Naming.tableNameForDimension(dimension);
        assertThat(result).isEqualTo("dimension_ProcedureCategory_cf2d4289");
    }

    @Test
    public void testTableNameForMultiValuedDimension() {
        String result = Naming.tableNameForMultiValuedDimension(factSet, "category", dimension);
        assertThat(result).isEqualTo("bridge_ProcedureRequest_f6bcea2b_category_ProcedureCategory_cf2d4289");
    }

    @Test
    public void testTableNameForFactSet() {
        String result = Naming.tableNameForFactSet(factSet);
        assertThat(result).isEqualTo("fact_ProcedureRequest_f6bcea2b");
    }

}