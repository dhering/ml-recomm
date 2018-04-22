package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.model.ml.{ProbabilitiesByState, StateProbability}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._

class ActionValueFunctionEstimatorSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {


  def fixture: {val session: SparkSession} = new {
    val session: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  feature("ActionValueFunctionEstimator") {

    scenario("estimate action value function") {

      Given("a list of 10 user transactions")
      val session = fixture.session
      import session.implicits._

      val transactions: DataFrame = Seq(
        ("A", "B", 0.5D),
        ("A", "C", 0.35D),
        ("A", "D", 0.15D),
        ("B", "D", 0.9999D),
        ("B", "E", 0.0001D),
        ("C", "F", 1.0D),
        ("D", "E", 1.0D),
        ("F", "G", 1.0D),
        ("G", "H", 1.0D),
        ("H", "I", 1.0D)
      ).toDF("antecedent", "consequent", "probability")

      val rewards = Map(
        "A" -> 5D,
        "B" -> 10D,
        "C" -> 15D,
        "D" -> 1D,
        "E" -> 10D,
        "F" -> 2D,
        "G" -> 1D,
        "H" -> 3D,
        "I" -> 1D
      )

      val estimator = new ActionValueFunctionEstimator(session, episodeEndingDepth = 4).setRewards(rewards)

      When("the transaction list is filtered")
      val model = estimator.fit(transactions).model.collect()

      Then("only 3 transactions should be remain")
      assert(model.length == 10)
      assert(getRowFor("A", "B", model).getDouble(2) ==
        10 * 0.5 // A -> B
          + scala.math.pow(0.5, 2) * 1 * 0.9999 // B -> D
          + scala.math.pow(0.5, 3) * 10 * 1.0 // D -> E
      )
      assert(getRowFor("A", "C", model).getDouble(2) ==
        15 * 0.35 // A -> C
          + scala.math.pow(0.5, 2) * 2 * 1.0 // C -> F
          + scala.math.pow(0.5, 3) * 1 * 1.0 // F -> G
          + scala.math.pow(0.5, 4) * 3 * 1.0 // G -> H
        // skip H -> I
      )
    }
  }

  feature("action value function") {

    scenario("estimate action value") {

      Given("a set of probabilities by states and rewards for the states")
      val probabilities = Map[String, ProbabilitiesByState](
        "A" -> ProbabilitiesByState("A", Seq(
          StateProbability("B", 0.5),
          StateProbability("C", 0.35),
          StateProbability("D", 0.15)
        )),
        "B" -> ProbabilitiesByState("B", Seq(
          StateProbability("E",0.9999D),
          StateProbability("E",0.0001D)
        )),
        "C" -> ProbabilitiesByState("C", Seq(StateProbability("F", 1.0D))),
        "D" -> ProbabilitiesByState("D", Seq(StateProbability("E", 1.0D))),
        "F" -> ProbabilitiesByState("F", Seq(StateProbability("G", 1.0D))),
        "G" -> ProbabilitiesByState("G", Seq(StateProbability("H", 1.0D))),
        "H" -> ProbabilitiesByState("H", Seq(StateProbability("I", 1.0D)))
      )

      val rewards = Map(
        "A" -> 5D,
        "B" -> 10D,
        "C" -> 15D,
        "D" -> 1D,
        "E" -> 10D,
        "F" -> 2D,
        "G" -> 1D,
        "H" -> 3D,
        "I" -> 1D
      )

      val estimator = new ActionValueFunctionEstimator(null, episodeEndingDepth = 4).setRewards(rewards)

      When("calculate action value for state 'C'")
      val actionValue = estimator.calculateActionValue(StateProbability("C", 0.35), probabilities, rewards, 1)

      Then("action value is as expected with a depth limit of 4")
      assert(actionValue ==
        2.0 * scala.math.pow(0.5, 0) * 0.35 // C -> F
          + 1.0 * scala.math.pow(0.5, 1) * 1.0 // F -> G
          + 3.0 * scala.math.pow(0.5, 2) * 1.0 // G -> H
          + 1.0 * scala.math.pow(0.5, 3) * 1.0 // H -> I
      )
    }
  }

  private def getRowFor(antecedent: String, consequent: String, rows: Array[Row]) = rows.collectFirst { case row
    if row.getString(0) == antecedent && row.getString(1) == consequent => row
  }.orNull
}
