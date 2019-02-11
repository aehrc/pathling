package au.csiro.clinsight.query.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.TreeSet;
import org.assertj.core.util.Lists;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class QueryPlanTest {

  @Test
  public void testJoinOrdering() {
    QueryPlan queryPlan = new QueryPlan();

    Join join1 = new Join("some expression 1", "someAlias1");
    Join join2 = new Join("some expression 2", "someAlias2");
    Join join3 = new Join("some expression 3", "someAlias3");
    Join join4 = new Join("some expression 4", "someAlias4");
    Join join5 = new Join("some expression 5", "someAlias5");
    join2.setDependsUpon(join1);
    join3.setDependsUpon(join1);

    queryPlan.setJoins(new TreeSet<>());
    queryPlan.getJoins().add(join2);
    queryPlan.getJoins().add(join4);
    queryPlan.getJoins().add(join1);
    queryPlan.getJoins().add(join3);
    queryPlan.getJoins().add(join5);

    List<Join> joins = Lists.newArrayList(queryPlan.getJoins());
    assertThat(joins.indexOf(join1)).isLessThan(joins.indexOf(join2));
    assertThat(joins.indexOf(join1)).isLessThan(joins.indexOf(join3));
  }
}