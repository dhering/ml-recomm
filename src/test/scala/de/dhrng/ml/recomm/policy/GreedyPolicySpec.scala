package de.dhrng.ml.recomm.policy

import de.dhrng.ml.recomm.model.ml.StateProbability
import org.scalatest._

class GreedyPolicySpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  feature("GreedyPolicy") {

    scenario("select greedy") {

      Given("a set of probabilities")
      def probabilities = Seq(
        StateProbability("A", 0.2), StateProbability("B", 0.7), StateProbability("A", 0.1)
      )

      When("the policy selects a probability")
      val probability = GreedyPolicy.select(probabilities)

      Then("the highest probability is chosen")
      assert(probability.state == "B")
      assert(probability.probability == 0.7D)
    }
  }
}
