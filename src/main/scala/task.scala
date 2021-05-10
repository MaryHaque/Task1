import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object task {

  def main(args: Array[String]): Unit = {


    val my_conf = new SparkConf().setAppName("Date_Generator_App").setMaster("local")

    val spark_session = SparkSession.builder().master("local[*]").config(conf = my_conf).getOrCreate()
    import spark_session.implicits._


    //=============================Date Difference Calculation=========================

    val today_date = java.time.LocalDate.now

    val start_calnder_date = LocalDate.parse("1970-01-01")

    val day_diffrence = ChronoUnit.DAYS.between(start_calnder_date, today_date).toInt - 1

    //=========================Calender Start Date Set And DataFrame Creation===========================================

    val start_date = Seq("1970-01-01")

    val date_df = spark_session.sparkContext.parallelize(start_date)

    var final_df_temp = date_df.toDF("start_date").withColumn("start_date", col("start_date").cast("Date"))

    var final_df_incr = final_df_temp


    //=====================For Loop to Generate Date Sequence From 1970-01-01===================

    for (i <- 1 to day_diffrence) {
      final_df_incr = final_df_incr.union(final_df_temp.withColumn("start_date", date_add(col("start_date"), i)));
    }


    final_df_incr.show(false)

    //=============================Adding Column Year,Month,Quarter===========================

    val date_year_month_quarter = final_df_incr.withColumn("Year", year(col("start_date"))).withColumn("Month", month(col("start_date"))).withColumn("Quarter", quarter(col("start_date"))).withColumn("Day", date_format(col("start_date"), "E"))

    //===Writing to CSV File=================================
    date_year_month_quarter.repartition(1).write.option("header", "true").mode("append").option("sep", "|").csv("file:///home/mhaque1/out_put_csv");

    //====================Writing to Parquet WIth Snappy Compression============

    date_year_month_quarter.repartition(1).write.option("spark.sql.parquet.compression.codec", "snappy").parquet("file:///home/mhaque1/out_put_csv_snappy");

    //====================Writing to Parquet WIth lz4 Compression============


    date_year_month_quarter.repartition(1).write.option("spark.sql.parquet.compression.codec", "lz4").parquet("file:///home/mhaque1/out_put_csv_lz4");

  }
}
