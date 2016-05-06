package Benchmarking;
 import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.chart.XYChart.Data;
import javafx.stage.Stage;
 public class Benchmarking extends Application {
 
	 public XYChart.Series<Number, Number> series1;
	 
 	enum RunType {
 		TRANSACTION_2PC,
 		REGULAR
 	}	
     @Override public void start(Stage stage) {
         stage.setTitle("Two Phase Commit");
         //defining the axes
         final NumberAxis xAxis = new NumberAxis();
         final NumberAxis yAxis = new NumberAxis();
         
         for(int a =0; a < 2; a++ ){
         
         xAxis.setLabel("# Operations");
         yAxis.setLabel("seconds");
         //creating the chart
         final LineChart<Number,Number> lineChart = 
                 new LineChart<Number,Number>(xAxis,yAxis);
                 
         lineChart.setTitle("Two Phase Commit Throughput");
         //defining a series
         series1 = new XYChart.Series<Number, Number>();
         //populating the series with data
         
         fetchDataPoints();
        
         Scene scene  = new Scene(lineChart,800,600);
         lineChart.getData().add(series1);
      
         stage.setScene(scene);
         
         stage.show();
         
         }
     }
  
     
     public void fetchDataPoints()
     {
    	 Mapper(3,4);
     }
     
     
	public void Mapper(int x, int y)
     {
			series1.getData().add(new Data<Number, Number>(x, y)); 
     }
     
     
     public static void main(String[] args) {
         launch(args);
     }
 }
 
 	