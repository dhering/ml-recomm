package de.dhrng.ml.recomm.agent

import de.dhrng.ml.recomm.model.ml.ActionValue
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class EGreedyPolicySpec extends FeatureSpec with GivenWhenThen with Matchers{

  feature("select recommendations") {

    scenario("select conclusion states without exploration") {

      Given("a list of 3 action values, ordered by value")
      val actionValues = ActionValue("A", 0.1) :: ActionValue("B", 0.2) :: ActionValue("C", 0.3) :: Nil
      val stateList = actionValues.map(_.state)


      When("3 recommendations are selected")
      val recommendations = new EGreedyPolicy(stateList, 0.0).select(actionValues, 3)

      Then("the states should appear in reverse order")
      assert(recommendations(0) == "C")
      assert(recommendations(1) == "B")
      assert(recommendations(2) == "A")
    }

    scenario("test exploration") {

      Given("a list of 12 action values, ordered by value")
      val actionValues =
        ActionValue("A", 0.10) ::
          ActionValue("B", 0.09) ::
          ActionValue("C", 0.08) ::
          ActionValue("D", 0.07) ::
          ActionValue("E", 0.06) ::
          ActionValue("F", 0.05) ::
          ActionValue("G", 0.04) ::
          ActionValue("H", 0.03) ::
          ActionValue("I", 0.02) ::
          ActionValue("J", 0.01) ::
          ActionValue("FOO", 0.0001) ::
          ActionValue("BAR", 0.0001) ::
          Nil
      val stateList = actionValues.map(_.state)

      And("initialize polycy with an epsilon of 10%")
      val policy = new EGreedyPolicy(stateList, 0.1)

      When("select 10 recommendations a 100 times")
      var foobars = 0
      var recommendationAmount = 0

      (0 until 100).foreach(_ => {
        val recommendations = policy.select(actionValues, 10)

        foobars += recommendations.count(state => state == "FOO" || state == "BAR")
        recommendationAmount += recommendations.size
      })

      Then("~ 10% of the recommendations should be random")
      val exporationPropotion = foobars.toDouble / recommendationAmount.toDouble
      assert(exporationPropotion === 0.1 +- 0.025)
    }
  }

  feature("select random state") {

    scenario("try to find random state without valid option") {

      Given("a list of 3 action values, ordered by value")
      val actionValues = ActionValue("A", 0.1) :: ActionValue("B", 0.2) :: ActionValue("C", 0.3) :: Nil
      val stateList = actionValues.map(_.state)
      val usedStates = stateList


      When("3 recommendations are selected")
      val state = new EGreedyPolicy(stateList, 0.0).randomState(usedStates)

      Then("the states should appear in reverse order")
      assert(state.isEmpty)
    }

    scenario("find random state out of 3 but 2 states are used") {

      Given("a list of 3 action values, ordered by value")
      val actionValues = ActionValue("A", 0.1) :: ActionValue("B", 0.2) :: ActionValue("C", 0.3) :: Nil
      val stateList = actionValues.map(_.state)
      val usedStates = stateList.filter(_ != "A")


      When("3 recommendations are selected")
      val state = new EGreedyPolicy(stateList, 0.0).randomState(usedStates)

      Then("the states should appear in reverse order")
      assert(state.isDefined)
      assert(state.get == "A")
    }
  }
}
