package TaskClass;

import org.apache.spark.sql.Column;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class GroupByClass {
	SparkSession sc;
	public GroupByClass(SparkSession sc) {
		// TODO Auto-generated constructor stub
		this.sc=sc;
	}

	public Dataset<Row> group(Dataset<Row> df, String s) {
		// TODO Auto-generated method stub
		String[] col=s.split("&");
		String[] key=col[0].trim().split(",");
		StructType schema=df.schema();
		String[] sumCol=col[1].split(",");
		Column[] gCol=new Column[key.length];
		for(int i=0;i<key.length;i++){
			gCol[i]=new Column(key[i]);
				
		}
		df=df.groupBy(gCol).sum(sumCol);
		for(int i=0;i<sumCol.length;i++){
			df.withColumnRenamed("sum("+sumCol[i]+")", sumCol[i]);
		}
		return df;
	}


}



