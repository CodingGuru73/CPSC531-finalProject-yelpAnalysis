package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class demo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("YelpAnalysis")
                .master("local[*]")
                .getOrCreate();

        // Path to the Yelp dataset Reveiws.JSON
        String path_reviews = "/home/will/project1/javaworld/yelpSpark/yelp_dataset/test_dataset/reviews.json";
        String path_business = "/home/will/project1/javaworld/yelpSpark/yelp_dataset/test_dataset/business.json";
        Dataset<Row> jsonDataReviews = spark.read().json(path_reviews);
        Dataset<Row> jsonDataBusiness = spark.read().json(path_business);
        // Dataset<Row> joineDataset = jsonDataReviews.join(jsonDataBusiness,
        // "business_id");
        // joineDataset.show();
        String business_name = "Abby Rappoport, LAC, CMQ";
        Dataset<Row> filteredBusiness = jsonDataBusiness.filter(jsonDataBusiness.col("name").equalTo(business_name));
        filteredBusiness.show();
    }
}
