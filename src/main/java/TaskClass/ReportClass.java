package TaskClass;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jfree.ui.RefineryUtilities;

import ChartPackage.BarGrapher;
import ProjectParameter.ParameterClass;
import scala.Function1;
import scala.Tuple2;


public class ReportClass implements Serializable{
	SparkSession sc;
	public ReportClass(SparkSession sc) {
		this.sc=sc;
	}
	public void generate(Dataset<Row> temp, final String s1,String s2) {
		// TODO Auto-generated method stub
		//temp.show();
		JavaRDD<Row> row=temp.toJavaRDD();
		JavaRDD<Row> rt=row.flatMap(new FlatMapFunction<Row,Row>(){
			String[] v=s1.split(",");
			public Iterator<Row> call(Row arg) throws Exception {
				// TODO Auto-generated method stub
				List<Row> lr=new ArrayList<Row>();
				Object[] obj=new Object[2];
				int i;
				String temp="";
				for(i=0;i<v.length;i++)
				{
					if(i==0)
					temp=temp+arg.getAs(v[i]);
					else
						temp=temp+","+arg.getAs(v[i]);
				}
				obj[0]=temp;
				obj[1]=(long)1;
				lr.add(RowFactory.create(obj));
				return lr.iterator();
			}
                	
       });
		List<StructField> fields = new ArrayList<StructField>();
		
		fields.add(DataTypes.createStructField(s1, DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> data=sc.createDataFrame(rt, schema).groupBy(s1).sum("count");
		//data.show();
		data=data.withColumnRenamed("sum(count)", "count");
		//data.show();
		//System.exit(0);
		long c=data.count();
		if(c<50){
			//display(data);
			data.show();
			
			//data.show();
			//System.exit(0);
			BarGrapher chart = new BarGrapher(data,s2,s1,"Statistics","Save The Chart",sc);
			      chart.pack( );        
			      RefineryUtilities.centerFrameOnScreen( chart );        
			      chart.setVisible( true );
			
		}
		else{
			System.out.println("Bar Graph Can Not Be Created");
		}
		//data=(data.withColumnRenamed("sum(count)", "count"));
		data.write().format("com.databricks.spark.csv").option("header", "true").mode(SaveMode.Overwrite).save(s2);
				
	}

}
