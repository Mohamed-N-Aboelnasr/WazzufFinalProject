
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.*;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.awt.*;
import java.util.List;


public class Model {

    public static void main(String []args){
        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder().appName("WAZZUF Jobs")
                .master("local[5]").getOrCreate();

        // Get DataFrameReader using SparkSession and set header option to true
        // to specify that first row in file contains name of columns
        final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
        // Loading Data from csv file
        final Dataset<Row> allData = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");
        // Displaying some records of the data
        allData.show();
        // Displaying structure of the data
        allData.printSchema();
        // Drop duplicates and null records
        allData.dropDuplicates().na().drop();
        // Create Temp view and execute query on it
        allData.createOrReplaceTempView("WAZZUF_Data");
        // Count the jobs for each company

        final Dataset<Row> companyJobs = sparkSession
                .sql("SELECT Company , COUNT(*) as no_of_jobs FROM WAZZUF_Data GROUP BY Company ORDER BY no_of_jobs DESC");
        // Showing the most demanding companies for jobs
        companyJobs.show();

        // Drawing a pie chart with the most demanding companies
        List<String> companies= companyJobs.select("Company").as(Encoders.STRING()).collectAsList();
        List<Double> no_jobs= companyJobs.select("no_of_jobs").as(Encoders.DOUBLE()).collectAsList();
        PieChart chart = new PieChartBuilder().width (800).height (600).title (Model.class.getSimpleName()).build ();
        Color[] sliceColors = new Color[]{new Color (226, 198, 20), new Color (203, 16, 46), new Color (80, 143, 160),new Color (180, 68, 50),new Color (66, 198, 20)};
        chart.getStyler ().setSeriesColors (sliceColors);
        for(int i=0;i<5;i++){
            chart.addSeries (companies.get(i), no_jobs.get(i));
        }
        // Showing the pie chart
        new SwingWrapper(chart).displayChart();

        // Find most popular job titles
        final Dataset<Row> popularJobs = sparkSession
                .sql("SELECT Title , COUNT(*) as demands FROM WAZZUF_Data GROUP BY Title ORDER BY demands DESC");
        // Showing the most demanding companies for jobs
        popularJobs.show();
        // Drawing a bar chart with the most demanding job titles
        List<String> titles= popularJobs.select("Title").as(Encoders.STRING()).collectAsList();
        List<Double> demands= popularJobs.select("demands").as(Encoders.DOUBLE()).collectAsList();
        // Create xchart to graph the most demanding job titles
        CategoryChart chart2 = new CategoryChartBuilder().width(1024).height(768).title("Most popular job titles").xAxisTitle("Job title").yAxisTitle("Demand").build();
        // Customize Chart
        chart2.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart2.getStyler().setHasAnnotations(true);
        chart2.getStyler().setStacked(true);
        // Adding the series
        chart2.addSeries("Job demands", titles.subList(0,10), demands.subList(0,10));
        // showing the bar chart
        new SwingWrapper(chart2).displayChart();
        // Find most popular areas


        final Dataset<Row> popularAreas = sparkSession
                .sql("SELECT Location , COUNT(*) as popularity FROM WAZZUF_Data GROUP BY Location ORDER BY popularity DESC");
        // Showing the most popular areas for jobs
        popularAreas.show();
        // Drawing a bar chart with the most popular areas for
        List<String> areas= popularAreas.select("Location").as(Encoders.STRING()).collectAsList();
        List<Double> popularity= popularAreas.select("popularity").as(Encoders.DOUBLE()).collectAsList();
        // Create xchart to graph the most popular areas for jobs
        CategoryChart chart3 = new CategoryChartBuilder().width(1024).height(768).title("Most popular area for jobs").xAxisTitle("Location").yAxisTitle("popularity").build();
        // Customize Chart
        chart3.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart3.getStyler().setHasAnnotations(true);
        chart3.getStyler().setStacked(true);
        // Adding the series
        chart3.addSeries("Areas Popularity", areas.subList(0,10), popularity.subList(0,10));
        // showing the bar chart
        new SwingWrapper(chart3).displayChart();
        // Find most demanding skills



        final Dataset<Row> skills = sparkSession
                .sql("SELECT Skill , COUNT(Skill) as no_repetitions FROM (SELECT EXPLODE(split(Skills,',')) AS Skill FROM WAZZUF_Data) GROUP BY(Skill) ORDER BY(no_repetitions) Desc");
        // Showing the most demanding skills
        skills.show();






    }
}
