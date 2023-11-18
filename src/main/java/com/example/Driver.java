package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Driver {
    public static void main(String[] args) {
        // create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("YelpAnalysis")
                .master("local[*]")
                .getOrCreate();

        // Path to the Yelp dataset Reveiws.JSON
        String path = "/home/will/project1/javaworld/yelpSpark/yelp_dataset/yelp_academic_dataset_review.json";
        Dataset<Row> jsonData = spark.read().json(path);

        /* only load first 100 rows to save cpu and memory */
        int numRowToLoad = 1000;
        Dataset<Row> limitedJsonData = jsonData.limit(numRowToLoad);

        // filter in a specific condition and show in a tabular format.
        // String filterCondition = "stars='3.0'";
        // Dataset<Row> filteredjsonData = limitedJsonData.filter(filterCondition);
        // filteredjsonData.show();

        // group columns by businessID, and then count each group
        // Dataset<Row> groupedJsonData_count =
        // limitedJsonData.groupBy("business_id").count();
        // groupedJsonData_count.show();

        /*
         * new Dataset<Row> with columns business_id, average ratings, review counts
         */
        Dataset<Row> groupedJsonData = limitedJsonData.groupBy("business_id").agg(
                avg("stars").as("average ratings"),
                count("text").as("review counts"));
        groupedJsonData.show();

        /*
         * get the specific value in the Dataset<Row>, e.g. get the average ratings of
         * the requested business_id
         * 1. using filter("business_id='requested business_id'"),
         * 2. using first() get the Row object of the filtered Dataset<Row>
         * 3. using getDouble(index=1), get the average ratings of the requested
         * business_id
         */

        System.out.println("+++++++++++++++++average values in the first row: " + groupedJsonData.first().getDouble(1));
        //System.out.println("+++++++++++++++++average values in the first row: " + groupedJsonData.first().getDouble(1));
    }
}
