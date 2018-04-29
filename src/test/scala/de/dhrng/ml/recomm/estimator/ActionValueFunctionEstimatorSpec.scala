package de.dhrng.ml.recomm.estimator

import de.dhrng.ml.recomm.model.ml.{ProbabilitiesByState, StateProbability}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._

class ActionValueFunctionEstimatorSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter  {


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

      And("an initialized estimator")
      val estimator = new ActionValueFunctionEstimator(session, episodeEndingDepth = 4).setRewards(rewards)

      When("the action value model is fitted")
      val model = estimator.fit(transactions).model.collect()

      Then("only 3 transactions should be remain")
      assert(model.length == 10)

      And("the action value from A to B is as expected")
      assert(getRowFor("A", "B", model).getDouble(2) ==
        10 * scala.math.pow(0.5, 0) * 0.5  // A -> B
          + 1 * scala.math.pow(0.5, 1) * 0.9999 // B -> D
          + 10 * scala.math.pow(0.5, 2) * 1.0, // D -> E
      "row[A, B]")

      And("the action value from A to C is as expected")
      assert(getRowFor("A", "C", model).getDouble(2) ==
        15.0 * scala.math.pow(0.5, 0) * 0.35 // A -> C
          + 2.0 * scala.math.pow(0.5, 1) * 1.0 // C -> F
          + 1.0 * scala.math.pow(0.5, 2) * 1.0 // F -> G
          + 3.0 * scala.math.pow(0.5, 3) * 1.0 // G -> H
          , // skip H -> I
      "row[A, C]")
    }
  }

  feature("action value function") {

    scenario("estimate action value") {

      Given("a set of probabilities by states and rewards for the states")
      val probabilitiesByStateA = ProbabilitiesByState("A", Seq(
        StateProbability("B", 0.5),
        StateProbability("C", 0.35),
        StateProbability("D", 0.15)
      ))
      val probabilitiesByStateB = ProbabilitiesByState("B", Seq(
        StateProbability("D",0.9999D),
        StateProbability("E",0.0001D)
      ))
      val probabilitiesByStateC = ProbabilitiesByState("C", Seq(StateProbability("F", 1.0D)))
      val probabilitiesByStateD = ProbabilitiesByState("D", Seq(StateProbability("E", 1.0D)))
      val probabilitiesByStateF = ProbabilitiesByState("F", Seq(StateProbability("G", 1.0D)))
      val probabilitiesByStateG = ProbabilitiesByState("G", Seq(StateProbability("H", 1.0D)))
      val probabilitiesByStateH = ProbabilitiesByState("H", Seq(StateProbability("I", 1.0D)))

      val estimator = Mockito.spy(new ActionValueFunctionEstimator(null, episodeEndingDepth = 4))
      doReturn(Some(probabilitiesByStateA)).when(estimator).getProbabilitiesByState("A")
      doReturn(Some(probabilitiesByStateB)).when(estimator).getProbabilitiesByState("B")
      doReturn(Some(probabilitiesByStateC)).when(estimator).getProbabilitiesByState("C")
      doReturn(Some(probabilitiesByStateD)).when(estimator).getProbabilitiesByState("D")
      doReturn(Some(probabilitiesByStateF)).when(estimator).getProbabilitiesByState("F")
      doReturn(Some(probabilitiesByStateG)).when(estimator).getProbabilitiesByState("G")
      doReturn(Some(probabilitiesByStateH)).when(estimator).getProbabilitiesByState("H")
      doReturn(None).when(estimator).getProbabilitiesByState("I")

      doReturn(5D) .when(estimator).getReward("A")
      doReturn(10D).when(estimator).getReward("B")
      doReturn(15D).when(estimator).getReward("C")
      doReturn(1D) .when(estimator).getReward("D")
      doReturn(10D).when(estimator).getReward("E")
      doReturn(2D) .when(estimator).getReward("F")
      doReturn(1D) .when(estimator).getReward("G")
      doReturn(3D) .when(estimator).getReward("H")
      doReturn(1D) .when(estimator).getReward("I")

      When("calculate action value for state 'F'")
      val actionValue = estimator.calculateActionValue("F", 1.0, 0)

      Then("action value is as expected with a depth limit of 4")
      assert(actionValue ==
        2.0 * scala.math.pow(0.5, 0) * 1.0 // C -> F
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
