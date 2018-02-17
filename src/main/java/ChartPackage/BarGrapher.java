package ChartPackage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel; 
import org.jfree.chart.JFreeChart; 
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset; 
import org.jfree.data.category.DefaultCategoryDataset; 
import org.jfree.ui.ApplicationFrame; 
import org.jfree.ui.RefineryUtilities; 

public class BarGrapher extends ApplicationFrame {
	String p;
   SparkSession sc;
   public BarGrapher(Dataset<Row> ds,String path,String parameter,String applicationTitle , String chartTitle, SparkSession sc ) {
      super( applicationTitle ); 
      p=parameter;
      this.sc=sc;
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Category",            
         "Count",            
         createDataset(ds),          
         PlotOrientation.VERTICAL,           
         true, true, false);
         
      ChartPanel chartPanel = new ChartPanel( barChart );        
      chartPanel.setPreferredSize(new java.awt.Dimension( 1000 , 700 ) );        
      setContentPane( chartPanel ); 
   }





private CategoryDataset createDataset(Dataset<Row> ds ) {
     
      final DefaultCategoryDataset dataset = 
      new DefaultCategoryDataset( ); 
      System.out.println("In BarGrapher"+"           "+ds.count());
      //final List<Row> lr=new ArrayList();
      List<Row> arrJoin=ds.collectAsList();
      for(int i=0;i<arrJoin.size();i++){
    	  	System.out.println("GHT");
      		Row r=arrJoin.get(i);
      		dataset.addValue(((Long)r.getAs("count")).doubleValue(),(String)r.getAs(p),p.toUpperCase());
    	  
      }
      //ds.collectAsList();
      //ds.coll
      //ds.collect();
      //Iterator it=ds.toLocalIterator();
      //ds.show();
      System.out.println("In BarGrapher"+"           "+ds.count());
      //System.exit(0);
     
      /*while(it.hasNext()){
    	  System.out.println("GHT");
    	Row r=(Row) it.next();
    	dataset.addValue(((Long)r.getAs("count")).doubleValue(),(String)r.getAs(p),p.toUpperCase());
    	
      }*/
      //System.exit(0);
      System.out.println("EXIT+++++++++++++++++++++");
         
                     

      return dataset; 
   }
   
 
}