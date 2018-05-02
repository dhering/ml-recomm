package de.dhrng.ml.recomm.writer

import java.io.{File, FileReader}

import org.apache.commons.csv.CSVFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

class CsvWriterSpec extends FeatureSpec with GivenWhenThen with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter {

  def fixture: {val session: SparkSession} = new {
    val session: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  before {
    val file = new File("target/test.csv")

    if(file.exists()){
      file.delete()
    }
  }

  feature("CsvWriter") {

    scenario("write dataframe as CSV") {

      Given("a list of 5 product transitions")
      val session = fixture.session
      import session.implicits._

      val data: DataFrame = Seq(
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
      ).toDF("antecedent", "consequent", "action_value")

      When("write dataframe to CSV file")
      CsvWriter.write(data, "target/test.csv")

      Then("a CSV file exists and is readable")
      val csvFile = CSVFormat.DEFAULT.withHeader().parse(new FileReader("target/test.csv"))

      And("there are three columns")
      assert(csvFile.getHeaderMap.size() == 3)

      And("the first row is A,B,0.5")
      val record = csvFile.getRecords.get(0)
      assert(record.get("antecedent") == "A")
      assert(record.get("consequent") == "B")
      assert(record.get("action_value") == "0.5")
    }
  }
}
