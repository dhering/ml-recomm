package de.dhrng.ml.recomm.agent

import de.dhrng.ml.recomm.model.ml.{ActionValue, ActionValuesByState}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class AgentSpec  extends FeatureSpec with GivenWhenThen with Matchers{

  feature("Agent") {

    scenario("predict recommendations with the agent") {

      Given("a action value model with one state 'X' with three action values")
      val actionValues = ActionValue("A", 0.1) :: ActionValue("B", 0.2) :: ActionValue("C", 0.3) :: Nil
      val model = Map("X" -> ActionValuesByState("X", actionValues))

      And("a recommendaton agent")
      val agent = new Agent(model)
      agent.policy.epsilon = 0.0

      When("the agent predict recommendations for state 'X'")
      val recommendations = agent.predict("X", 3)

      Then("three recommendations should be appear")
      assert(recommendations(0) == "C")
      assert(recommendations(1) == "B")
      assert(recommendations(2) == "A")
    }
  }
}
