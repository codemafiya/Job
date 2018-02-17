package TaskClass;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrderByClass {
	SparkSession sc;
	public OrderByClass(SparkSession sc) {
		// TODO Auto-generated constructor stub
		this.sc=sc;
	}

	public Dataset<Row> order(Dataset<Row> df, String string) {
		String k[]=string.split(",");
		for(int i=0;i<k.length;i++){
			df=df.orderBy(df.col(k[i]));
		}
		
		return df;
	}

}
