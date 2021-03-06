package de.dhrng.ml.recomm.transformer

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._

class FrequentItemSetTransformerSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  def fixture: {
    val session: SparkSession
    val transactions: DataFrame
  } = new {
      val session: SparkSession = SparkSession
        .builder()
        .appName("test")
        .master("local[1]")
        .getOrCreate()

      import session.implicits._

      val transactions: DataFrame = Seq(
        ("trans1", "A"), ("trans1", "A"), ("trans1", "B"), ("trans1", "C"),
        ("trans2", "A"), ("trans2", "B"), ("trans2", "D")
      ).toDF("transactID", "itemID")
    }

  feature("FrequentItemSetTransformer") {

    scenario("transform translogs to frequent itemsets") {

      Given("transactions for 2 user sessions")
      val transactions = fixture.transactions

      And("an initialized transformer")
      val session = fixture.session
      val frequentItemSetTransformer = new FrequentItemSetTransformer(session )

      When("the transactions are transformed into frequency itemsets")
      val dataFrame = frequentItemSetTransformer.transform(transactions)

      Then("3 itemsets should be there")
      val rows = dataFrame.collect()
      assert(rows.length == 3)

      And("the frequency for A to B is 2")
      assert(getRowFor("A", "B", rows).getLong(2) == 2)

      And("the frequency for B to C is 1")
      assert(getRowFor("B", "C", rows).getLong(2) == 1)

      And("the frequency for B to D is 1")
      assert(getRowFor("B", "D", rows).getLong(2) == 1)
    }


    scenario("transform to frequent itemsets with marked endings") {

      Given("transactions for 2 user sessions")
      val session = fixture.session

      val transactions = fixture.transactions

      val frequentItemSetTransformer = new FrequentItemSetTransformer(session, markEnding = true)

      When("the transactions are transformed into frequency itemsets")
      val dataFrame = frequentItemSetTransformer.transform(transactions)

      Then("4 itemsets should be there")
      val rows = dataFrame.collect()

      assert(dataFrame.count() == 5)
      assert(getRowFor("A", "B", rows).getLong(2) == 2)
      assert(getRowFor("B", "C", rows).getLong(2) == 1)
      assert(getRowFor("B", "D", rows).getLong(2) == 1)
      assert(getRowFor("C", "#END#", rows).getLong(2) == 1)
      assert(getRowFor("D", "#END#", rows).getLong(2) == 1)
    }
  }

  private def getRowFor(antecedent: String, consequent: String, rows: Array[Row]) = rows.collectFirst { case row
    if row.getString(0) == antecedent && row.getString(1) == consequent => row
  }.orNull
}
