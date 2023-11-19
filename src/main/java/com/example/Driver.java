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

                
        // Path to the Yelp dataset Reveiws.JSON, and business.json
        String path_reviews = "/home/will/project1/javaworld/yelpSpark/yelp_dataset/yelp_academic_dataset_review.json";
        String path_business = "/home/will/project1/javaworld/yelpSpark/yelp_dataset/yelp_academic_dataset_business.json";
        Dataset<Row> jsonDataReviews = spark.read().json(path_reviews);
        Dataset<Row> jsonDataBusiness = spark.read().json(path_business);
        // only load first 1000 rows to save cpu and memory
        int numRowToLoad = 1000;
        Dataset<Row> limitedReviewsJsonData = jsonDataReviews.limit(numRowToLoad);
        Dataset<Row> limitedBusinessnJsonData = jsonDataBusiness.limit(numRowToLoad);
        // show the dataset in a tabular way in the console
        limitedReviewsJsonData.show();
        limitedBusinessnJsonData.show();
        // search a business by business name, and get the stars and reviews count of
        // it.
        String business_name = "Tsevi's Pub And Grill";
        Dataset<Row> filteredBusinessData = limitedBusinessnJsonData
                .filter(limitedBusinessnJsonData.col("name").equalTo(business_name));
        filteredBusinessData.show();
        System.out.println("+++++++++++++++++rating stars of the business: " +
                filteredBusinessData.first().getAs("stars"));
        System.out.println("+++++++++++++++++reviews counts of the business: " +
                filteredBusinessData.first().getAs("review_count"));

        // how to find pros and cons to a business from review.json




        

        /*
         * new Dataset<Row> joining business.json with reviews.json
         * it would only show the dataset with common business_id
         */

        /*
         * Dataset<Row> joinedDataset =
         * limitedBusinessnJsonData.join(limitedReviewsJsonData, "business_id");
         * System.out.println("joined dataset: ");
         * joinedDataset.show();
         */
        /*
         * new Dataset<Row> with columns business_id, average ratings, review counts
         */
        /*
         * Dataset<Row> groupedJsonData =
         * limitedReviewsJsonData.groupBy("business_id").agg(
         * avg("stars").as("average ratings"),
         * count("text").as("review counts"));
         */
        // groupedJsonData.show();

        /*
         * get the specific value in the Dataset<Row>, e.g. get the average ratings of
         * the requested business_id
         * 1. using filter("business_id='requested business_id'"),
         * 2. using first() get the Row object of the filtered Dataset<Row>
         * 3. using getDouble(index=1), get the average ratings of the requested
         * business_id
         */

        // System.out.println("+++++++++++++++++average values in the first row: " +
        // groupedJsonData.first().getDouble(1));
        // System.out.println("+++++++++++++++++average values in the first row: " +
        // groupedJsonData.first().getDouble(1));
    }
}
