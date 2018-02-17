package TaskClass;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ShowClass {
	SparkSession spark;
	public ShowClass(SparkSession spark) {
		// TODO Auto-generated constructor stub
		this.spark=spark;
	}


	public Dataset<Row> showDataFrame(Dataset<Row> ds){
		ds.show();
		return ds;
	}

}
