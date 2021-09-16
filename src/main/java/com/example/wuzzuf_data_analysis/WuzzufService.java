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

    public List<String> DemandingCompanies() throws IOException {
        data=checkData().dropDuplicates().na().drop();
        // Create view and execute query to convert types as, by default, all columns have string types
        data.createOrReplaceTempView ("COMPANY_JOBSs");
        final Dataset<Row> companyJobsData = sparkSession
                .sql ("SELECT Company, COUNT(*) DemandedJobsNo FROM COMPANY_JOBSs GROUP BY Company ORDER BY DemandedJobsNo DESC");

        PieChart chart = new PieChartBuilder().width (1024).height (728).title ("Top Ten Demanding Companies For Jobs").build ();
        // Customize Chart
        Color[] sliceColors = new Color[]{
                new Color (111, 46, 67),
                new Color (204, 77, 88),
                new Color (243, 101, 35),
                new Color (245, 149, 29),
                new Color (249, 194, 50),
                new Color (111, 156, 51),
                new Color (43, 138, 134),
                new Color (73, 114, 136),
                new Color (22, 87, 141),
                new Color (95, 77, 153)};
        chart.getStyler ().setSeriesColors (sliceColors);

        //Demanded Jobs Number Column
        List<Row> JobArray = companyJobsData.select("DemandedJobsNo").collectAsList();
        Long[] job_Num = new Long[JobArray.size()];
        for (int i = 0; i < JobArray.size(); i++)
            job_Num[i] = (Long) JobArray.get(i).get(0);
        int[] Num_values = Arrays.stream(job_Num).mapToInt(x -> x.intValue()).toArray();

        //Company Column
        List<Row> CompanyArray= companyJobsData.select("Company").collectAsList();
        String[] companies = new String[10];
        for (int i = 0; i < 10; i++)
            companies[i] = (String) CompanyArray.get(i).get(0);

        // Series
        for (int i = 0; i <10; i++)
        {
            chart.addSeries(companies[i], Num_values[i]);
        }

        try {
            BitmapEncoder.saveBitmap(chart, "src/main/resources/charts/company.png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return companyJobsData.limit(20).toJSON().collectAsList();
    }

    public List<String> mostPopJobs(){
        data=checkData().dropDuplicates().na().drop();
        data.createOrReplaceTempView("Wuzzuf_Jobs");
        final Dataset<Row> most_pop_jobs = sparkSession.sql("SELECT cast(Title as string) Title, cast(COUNT(*) as int) MostPopularJobTitles FROM Wuzzuf_Jobs GROUP BY Title ORDER BY MostPopularJobTitles DESC");
        List<Row> listOfPopJobs = most_pop_jobs.takeAsList(10); //(int) most_pop_jobs.count()
        // store indices of the list in another two lists
        String[] titles = listOfPopJobs.stream().map(s -> s.get(0)).collect(Collectors.toList()).toArray(String[]::new);
        Integer[] titles_count = listOfPopJobs.stream().map(s -> s.get(1)).collect(Collectors.toList()).toArray(Integer[]::new);
        getBarChart("Most-Popular-Job-Titles", "Job Title", "Number of Demand", titles, titles_count);
        return most_pop_jobs.limit(20).toJSON().collectAsList();

    }

    public List<String> mostPopArea(){
        data=checkData().dropDuplicates().na().drop();
        data.createOrReplaceTempView("Wuzzuf_Jobs");
        final Dataset<Row> most_pop_locs = sparkSession.sql(" SELECT cast(Location as string) Location, cast(COUNT(*) as int) MostPopularAreas FROM Wuzzuf_Jobs GROUP BY Location ORDER BY MostPopularAreas DESC");
        List<Row> listOfPopAreas = most_pop_locs.takeAsList(10); //(int) most_pop_locs.count()
        // store indices of the list in another two lists
        String[] areas = listOfPopAreas.stream().map(s -> s.get(0)).collect(Collectors.toList()).toArray(String[]::new);
        Integer[] areas_count = listOfPopAreas.stream().map(s -> s.get(1)).collect(Collectors.toList()).toArray(Integer[]::new);

        getBarChart("Most-Popular-Areas", "Location", "Number of Demand", areas, areas_count);

        return most_pop_locs.limit(20).toJSON().collectAsList();

    }

    public List<Map.Entry> getSkills(){
        // get most popular skills with a sql query
        data=checkData().dropDuplicates().na().drop();
        data.createOrReplaceTempView("Wuzzuf_skills");
        final Dataset<Row> most_pop_skills = sparkSession.sql("SELECT cast(Skills as string) Skills FROM Wuzzuf_skills");

        // convert dataset to javaRDD
        JavaRDD<String> most_pop_skills_column = most_pop_skills.toJavaRDD().map(f -> f.toString());

        // map RDD, split strings, handle all
        JavaRDD<String> all_rows_skills = most_pop_skills_column.flatMap (skill -> Arrays.asList (skill
                .toLowerCase()
                .trim ()
                .replaceAll ("\\[", "").replaceAll("\\]", "").replaceAll ("\\<", "")
                .split (", ")).iterator ());

        // add skill and count in a Map
        Map<String, Long> skill_count = all_rows_skills.countByValue();

        // sort in descending order
        List<Map.Entry> popular_sorted_skills = skill_count.entrySet ().stream ()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect (Collectors.toList ());

        return popular_sorted_skills;
    }

    private void getBarChart(String title, String x_axis, String y_axis, String[] x_col, Integer[] y_col) {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).theme(ChartTheme.GGPlot2).xAxisTitle(x_axis).yAxisTitle(y_axis).build();
        
        // setting color palette
        Color[] seriesColors = new Color[]{
            new Color (204, 77, 88), 
            new Color (111, 46, 67),
            new Color (243, 101, 35),
            new Color (245, 149, 29),
            new Color (249, 194, 50),
            new Color (111, 156, 51),
            new Color (43, 138, 134),
            new Color (73, 114, 136),
            new Color (22, 87, 141),
            new Color (95, 77, 153)};
        
        // Customize Chart
        chart.getStyler().setLegendPosition(LegendPosition.InsideNE);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setAnnotationsFontColor(Color.WHITE);
        chart.getStyler().setXAxisLabelRotation(45);
        Font bold = Font.decode("BOLD");
        chart.getStyler().setAnnotationsFont(bold);
        chart.getStyler().setStacked (true);
        chart.getStyler().setSeriesColors(seriesColors);

        // Series
        chart.addSeries(x_axis, Arrays.asList(x_col), Arrays.asList(y_col));


        try {
            BitmapEncoder.saveBitmap(chart, "src/main/resources/charts/"+title+".png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
