package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.{ActionValue, StateProbability}
import org.scalatest._

class GreedyPolicySpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  feature("GreedyPolicy") {

    scenario("select greedy") {

      Given("a set of probabilities")
      def actionValues = Seq(
        ActionValue("A", 0.2), ActionValue("B", 0.7), ActionValue("A", 0.1)
      )

      When("the policy selects a probability")
      val probability = GreedyPolicy.select(actionValues)

      Then("the highest probability is chosen")
      assert(probability.state == "B")
      assert(probability.value == 0.7D)
    }
  }
}
