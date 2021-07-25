package com.example.wuzzuf_data_analysis;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;



@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;
    @Value("${dataPath}")
    private String wuzzufPath;

    @Bean
    public SparkSession session() {
        return  SparkSession.builder ().appName (appName).master (masterUri).getOrCreate ();
    }

    @Bean
    public Dataset<Row> Dataset() {
        return session().read ().option ("header", "true").csv (wuzzufPath);//.dropDuplicates().na().drop();
    }

}