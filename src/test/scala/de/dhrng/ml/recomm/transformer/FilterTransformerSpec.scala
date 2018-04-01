package de.dhrng.ml.recomm.transformer

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

class FilterTransformerSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  def fixture =
    new {
      val session =  SparkSession
        .builder()
        .appName("test")
        .master("local[1]")
        .getOrCreate()
    }

  feature("FilterTransformer") {

    scenario("filter transactions") {

      Given("a list of 5 user transactions")
      val session = fixture.session
      import session.implicits._

      val transactions: DataFrame = Seq(
        ("trans1", "A", -1),
        ("trans1", "B", -1),
        ("trans1", "__Z", -1),  // remove this
        ("trans1", "C", -1),
        ("trans1", "C", 1)      // remove this
      ).toDF("transactID", "itemID", "transType")

      val filterTransformer = new FilterTransformer(session )

      When("the transaction list is filtered")
      val dataFrame = filterTransformer.transform(transactions)

      Then("only 3 transactions should be remain")
      assert(dataFrame.count() == 3)
    }
  }
}
