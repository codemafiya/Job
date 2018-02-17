package TaskClass;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class FilterClass {
	SparkSession sc;
	public FilterClass(SparkSession sc) {
		// TODO Auto-generated constructor stub
		this.sc=sc;
	}

	public Dataset<Row> filter(Dataset<Row> df,String s){
		String[] col=s.split("&");
		StructType schema=df.schema(); 
		for(int i=0;i<col.length;i++){
			String f[]=col[i].split("=");
			String type=schema.typeName().toString();
			df=df.filter(df.col(f[0]).equalTo(f[1]));
		}
		return df;
	}

}
