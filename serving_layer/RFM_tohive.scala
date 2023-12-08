/*
The RFM model is an important tool and means to measure customer value and customer profit-making ability. Three elements constitute the best indicators for data analysis, namely:
R-Recency (last purchase time)
F-Frequency (consumption frequency)
M-Money (consumption amount)
*/

// --R-Recency (last purchase time), the higher the R value, it generally indicates that the user is more active
// --F-Frequency (consumption frequency), the higher the F value, the more loyal the user is


import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window


// Assuming your data is stored in a DataFrame named userBehaviorDF
val userBehaviorDF = spark.table("xqoasis_category_behavior")// Load or create your DataFrame here

// Transformations to calculate R and F values
// No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
val cteRDD: RDD[(String, Int, Int, Long, Int)] = userBehaviorDF.
       filter($"behavior_type" === "buy").
       groupBy("user_id").
       agg(
       datediff(lit("2017-12-04"), max("behavior_date")).alias("R"),
       dense_rank().over(Window.orderBy(datediff(lit("2017-12-04"), max("behavior_date")))).alias("R_rank"),
       count("*").alias("F"),
       dense_rank().over(Window.orderBy(count("*").desc)).alias("F_rank")
       ).
       rdd.
       map(row => (row.getString(0), row.getInt(1), row.getInt(2), row.getLong(3), row.getInt(4)))

// Create a temporary view for further usage
val xqoasisUserCentCountRDD: RDD[(String, Int, Int, Long, Int)] = cteRDD.cache()
// Convert RDD to DataFrame and register it as a temporary view
val xqoasisUserCentCountDF = xqoasisUserCentCountRDD.toDF("user_id", "R", "R_rank", "F", "F_rank")
xqoasisUserCentCountDF.createOrReplaceTempView("xqoasis_user_cent_count")


// --Rate the user
/*
Users with purchasing behavior are grouped according to their ranking, and divided into 5 groups in total.
Top - 1/5 users rated it 5
Top 1/5 - 2/5 users give 4 points
Top 2/5 - 3/5 users give 3 points
The first 3/5 - 4/5 users give 2 points
Top 4/5 - users rated it 1
According to this rule, the user's time interval ranking and purchase frequency ranking are scored respectively, and finally the two scores are combined together as the final score of the user.
*/
// Transformations to calculate R_score and F_score, and merge the results
def rankToScore(rank: Int): Int = rank match {
  case 1 => 5
  case 2 => 4
  case 3 => 3
  case 4 => 2
  case 5 => 1
  case _ => 0
}

val xqoasisUserScoreMergeRDD: RDD[(String, Int, Int, Int, Long, Int, Int, Int)] = xqoasisUserCentCountRDD.
  map { case (user_id, r, r_rank, f, f_rank) =>
    // Calculate R_score and F_score based on rank
    val r_score = rankToScore(r_rank)
    val f_score = rankToScore(f_rank)

    // Calculate the total score
    val score = r_score + f_score

    (user_id, r, r_rank, r_score, f, f_rank, f_score, score)
  }.sortBy(_._8, false)


// Create a temporary view for further usage
val xqoasisUserScoreMergeRDD: RDD[(String, Int, Int, Int, Long, Int, Int, Int)] = xqoasisUserScoreMergeRDD.cache()
// Convert RDD to DataFrame and register it as a temporary view
val xqoasisUserScoreMergeDF = xqoasisUserScoreMergeRDD.toDF("user_id", "R", "R_rank", "R_score", "F", "F_rank", "F_score", "score")
xqoasisUserScoreMergeDF.createOrReplaceTempView("xqoasis_user_score_merge")
// write into Hive table
xqoasisUserScoreMergeDF.write.mode("overwrite").saveAsTable("xqoasis_user_score_merge")

// check
xqoasisUserScoreMergeDF.show(5)
