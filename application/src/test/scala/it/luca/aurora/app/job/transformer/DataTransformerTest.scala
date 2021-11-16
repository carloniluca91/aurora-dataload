package it.luca.aurora.app.job.transformer

import it.luca.aurora.app.job.transformer.DataTransformerTest._
import it.luca.aurora.configuration.metadata.extract.{AvroExtract, Extract}
import it.luca.aurora.configuration.metadata.transform.{FileNamePartitioning, Transform}
import it.luca.aurora.core.SparkBasicTest
import it.luca.aurora.core.implicits.DataFrameWrapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class DataTransformerTest
  extends SparkBasicTest {

  private val fileNameDate: String = LocalDate.now().format(DateTimeFormatter.ofPattern(FileNameDatePattern))
  private val filePath: Path = new Path(s"/user/data/fileName_$fileNameDate.avro")
  private val fileNameRegex: String = "^fileName_(\\d{8}).avro$"
  private val extract: AvroExtract = AvroExtract(Extract.Avro, "ignore", fileNameRegex)
  private val partitioningColumnName: String = "dt_business_date"
  private val partitioning: FileNamePartitioning = FileNamePartitioning("IGNORE", partitioningColumnName, 1, FileNameDatePattern, DateOutputPattern)
  private val filters: Seq[String] =
    s"${TestClass.FirstColumn} IS NOT NULL" ::
      s"${TestClass.SecondColumn} IS NOT NULL" :: Nil

  implicit class DataFrameTestHelper(protected val dataFrame: DataFrame) {

    def shouldContainColumn(colName: String): Unit =
      dataFrame.columns.contains(colName) shouldBe true

    def shouldNotContainColumn(colName: String): Unit =
      dataFrame.columns.contains(colName) shouldBe false

    def shouldContainTechnicalColumns(): Unit = {

      shouldContainColumn(DataFrameWrapper.InsertTs)
      shouldContainColumn(DataFrameWrapper.InsertDt)
      shouldContainColumn(DataFrameWrapper.ApplicationId)
      shouldContainColumn(DataFrameWrapper.ApplicationName)
      shouldContainColumn(DataFrameWrapper.ApplicationStartTime)
      shouldContainColumn(DataFrameWrapper.ApplicationStartDate)
    }

    def lastColumnShouldBe(colName: String): Unit =
      dataFrame.columns.last shouldBe colName

    def uniqueValueOfColumnShouldBe[T](colName: String, extractor: Row => T, expected: T): Unit = {

      val rows: Seq[Row] = dataFrame.select(colName).distinct().collect()
      rows.size shouldBe 1
      extractor(rows.head) shouldBe expected
    }
  }

  protected def toDF(data: Seq[TestClass]): DataFrame = {
    import sparkSession.implicits._
    data.toDF(TestClass.Id, TestClass.FirstColumn, TestClass.SecondColumn)
  }

  s"A ${nameOf[DataTransformer]}" should "correctly transform both trusted and error data" in {

    val inputDataFrame: DataFrame = toDF(
      TestClass(1, Some("hello"), Some("world")) ::
        TestClass(2, None, None) :: Nil)

    val transformations: Seq[String] =
        s"${TestClass.FirstColumn}" ::
        s"${TestClass.SecondColumn}" :: Nil

    val transform: Transform = Transform(filters, transformations, None, None, partitioning)
    val (trustedDf, errorDf): (DataFrame, DataFrame) = new DataTransformer(extract, transform).transform(inputDataFrame, filePath)

    // Test shape of trusted data and size
    trustedDf.shouldContainColumn(TestClass.FirstColumn)
    trustedDf.shouldContainColumn(TestClass.SecondColumn)
    trustedDf.shouldContainColumn(DataTransformer.InputFilePath)
    trustedDf.uniqueValueOfColumnShouldBe(DataTransformer.InputFilePath, _.getString(0), filePath.toString)
    trustedDf.shouldContainTechnicalColumns()
    trustedDf.lastColumnShouldBe(partitioningColumnName)
    trustedDf.count() shouldBe 1

    // Test shape of error data and size
    errorDf.shouldContainColumn(TestClass.Id)
    errorDf.shouldContainColumn(TestClass.FirstColumn)
    errorDf.shouldContainColumn(TestClass.SecondColumn)
    errorDf.shouldContainColumn(DataTransformer.FailedCheckCount)
    errorDf.shouldContainColumn(DataTransformer.FailedChecks)
    errorDf.shouldContainColumn(DataTransformer.InputFilePath)
    errorDf.uniqueValueOfColumnShouldBe(DataTransformer.InputFilePath, _.getString(0), filePath.toString)
    errorDf.shouldContainTechnicalColumns()
    errorDf.lastColumnShouldBe(partitioningColumnName)
    errorDf.count() shouldBe 1
  }

  it should "correctly transform trusted data when duplicates need to be removed" in {

    // duplicates for 'id' column
    val inputDataFrame: DataFrame = toDF(
      TestClass(1, Some("hello"), Some("world")) ::
        TestClass(1, Some("ohiOhi"), Some("ilBudello")) ::
        TestClass(2, None, None) :: Nil)

    val filters: Seq[String] =
      s"${TestClass.FirstColumn} IS NOT NULL" ::
        s"${TestClass.SecondColumn} IS NOT NULL" :: Nil

    val transformations: Seq[String] =
      s"${TestClass.Id}" ::
        s"${TestClass.FirstColumn}" ::
        s"${TestClass.SecondColumn}" :: Nil

    // Set 'id' as column to be used for duplicates removal
    val transform: Transform = Transform(filters,
      transformations,
      dropDuplicates = Some(TestClass.Id :: Nil),
      dropColumns = None,
      partitioning = partitioning)

    val (trustedDf, _): (DataFrame, DataFrame) = new DataTransformer(extract, transform).transform(inputDataFrame, filePath)
    trustedDf.shouldContainColumn(TestClass.Id)
    trustedDf.shouldContainColumn(TestClass.FirstColumn)
    trustedDf.shouldContainColumn(TestClass.SecondColumn)
    trustedDf.count() shouldBe 1
  }

  it should "correctly transform trusted data when duplicates need to be removed and columns need to be dropped" in {

    // duplicates for 'id' column
    val inputDataFrame: DataFrame = toDF(
      TestClass(1, Some("hello"), Some("world")) ::
        TestClass(1, Some("ohiOhi"), Some("ilBudello")) ::
        TestClass(2, None, None) :: Nil)

    val filters: Seq[String] =
      s"${TestClass.FirstColumn} IS NOT NULL" ::
        s"${TestClass.SecondColumn} IS NOT NULL" :: Nil

    val transformations: Seq[String] =
      s"${TestClass.Id}" ::
        s"${TestClass.FirstColumn}" ::
        s"${TestClass.SecondColumn}" :: Nil

    // Set 'id' as column to be used for duplicates removal and then drop it
    val transform: Transform = Transform(filters,
      transformations,
      dropDuplicates = Some(TestClass.Id :: Nil),
      dropColumns = Some(TestClass.Id :: Nil),
      partitioning = partitioning)

    val (trustedDf, _): (DataFrame, DataFrame) = new DataTransformer(extract, transform).transform(inputDataFrame, filePath)

    trustedDf.shouldNotContainColumn(TestClass.Id)
    trustedDf.shouldContainColumn(TestClass.FirstColumn)
    trustedDf.shouldContainColumn(TestClass.SecondColumn)
    trustedDf.count() shouldBe 1
  }
}

object DataTransformerTest {

  val FileNameDatePattern: String = "yyyyMMdd"
  val DateOutputPattern = "yyyy-MM-dd"
}
