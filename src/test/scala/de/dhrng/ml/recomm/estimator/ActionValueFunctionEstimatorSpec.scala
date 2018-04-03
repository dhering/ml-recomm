package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.estimator.ActionValueFunctionEstimator.{ProbabilitiesByState, Probability}
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
      ).toDF("antecedent", "consequent", "confidence")

      val pricing = Map(
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

      val estimator = new ActionValueFunctionEstimator(session, episodeEndingDepth = 4).setPricing(pricing)

      When("the transaction list is filtered")
      val model = estimator.fit(transactions).model.collect()

      Then("only 3 transactions should be remain")
      assert(model.length == 10)
      assert(getRowFor("A", "B", model).getDouble(2) ==
        0.5 // A -> B
          + scala.math.pow(0.5, 2) * 1 * 0.9999 // B -> D
          + scala.math.pow(0.5, 3) * 10 * 1.0 // D -> E
      )
      assert(getRowFor("A", "C", model).getDouble(2) ==
        0.35 // A -> C
          + scala.math.pow(0.5, 2) * 2 * 1.0 // C -> F
          + scala.math.pow(0.5, 3) * 1 * 1.0 // F -> G
          + scala.math.pow(0.5, 4) * 3 * 1.0 // G -> H
        // skip H -> I
      )
    }
  }

  feature("action value function") {

    scenario("estimate action value") {

      Given("a set of probabilities by states and pricing for the states")
      val probabilities = Map[String, ProbabilitiesByState](
        "A" -> ProbabilitiesByState("A", Seq(
          Probability(0.5, "B"),
          Probability(0.35, "C"),
          Probability(0.15, "D")
        )),
        "B" -> ProbabilitiesByState("B", Seq(
          Probability(0.9999D, "E"),
          Probability(0.0001D, "E")
        )),
        "C" -> ProbabilitiesByState("C", Seq(Probability(1.0D, "F"))),
        "D" -> ProbabilitiesByState("D", Seq(Probability(1.0D, "E"))),
        "F" -> ProbabilitiesByState("F", Seq(Probability(1.0D, "G"))),
        "G" -> ProbabilitiesByState("G", Seq(Probability(1.0D, "H"))),
        "H" -> ProbabilitiesByState("H", Seq(Probability(1.0D, "I")))
      )

      val pricing = Map(
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

      val estimator = new ActionValueFunctionEstimator(null, episodeEndingDepth = 4).setPricing(pricing)

      When("calculate action value for state 'C'")
      val actionValue = estimator.calculateActionValue("C", probabilities, pricing, 2)

      Then("action value is as expected with a depth limit of 4")
      assert(actionValue ==
        scala.math.pow(0.5, 2) * 2 * 1.0 // C -> F
          + scala.math.pow(0.5, 3) * 1 * 1.0 // F -> G
          + scala.math.pow(0.5, 4) * 3 * 1.0 // G -> H
      )
    }
  }

  private def getRowFor(antecedent: String, consequent: String, rows: Array[Row]) = rows.collectFirst { case row
    if row.getString(0) == antecedent && row.getString(1) == consequent => row
  }.orNull
}
