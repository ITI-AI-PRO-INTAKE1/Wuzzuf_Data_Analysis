package com.example.wuzzuf_data_analysis;




import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Service
public class WuzzufService {
    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;
    @Value("src/main/resources/static/Wuzzuf_Jobs.csv")
    private  String wuzzufPath;

    @Autowired
    Dataset<Row> dataset;
    @Autowired
    SparkSession sparkSession;
    private Dataset<Row> data=null;

    private Dataset<Row> checkData(){
        if(data==null)
            return dataset;
        else
            return data;
    }


    public List<String> getSummary() {
        data= checkData();
        return data.describe().toJSON().collectAsList() ;
    }

    public List<String> clean(){
        data= checkData().dropDuplicates().na().drop();
        return data.toJSON().collectAsList();
    }

    public String getStructure () {
        data=checkData();
        return  data.schema().mkString(", ");
    }

    public List<String> getData(int num) {
        data=checkData();
        return data.limit(num).toJSON().collectAsList();
    }

    public List<String> factorizeYearsExp(){
        data=checkData().dropDuplicates().na().drop();
        Dataset<Row>df= new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("FactorizeYearsExp").fit(data).transform(data);

        return df.toJSON().collectAsList();
    }

    public void DemandingCompanies() throws IOException {
    }

    public void mostPopJobs(){

    }

    public void mostPopArea(){


    }

    public void getSkills(){

    }

    private void getBarChart(String title, String x_axis, String y_axis, String[] x_col, Integer[] y_col) {



    }


}
