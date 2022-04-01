package com.risingwave.planner;

import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Contains query plan for distributed join contains broadcast, e.g. nested loop join. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BroadcastJoinPlannerTest extends BatchPlanTestBase {

  @BeforeAll
  public void initAll() {
    super.init();
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Broadcast join plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testBroadCastJoinPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCase(testCase);
  }
}
