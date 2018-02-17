package TaskClass;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SelectClass {
	SparkSession sc;
	public SelectClass(SparkSession sc) {
		// TODO Auto-generated constructor stub
		this.sc=sc;
		
	}

	public Dataset<Row> select(Dataset<Row> df,String s){
		String col[]=s.split(",");
		Column[] colss=new Column[col.length];
		for(int i=0;i<col.length;i++){
			Column cols=new Column(col[i]);
			colss[i]=cols;
		}
		df=df.select(colss);
		return df;
		
	}

}
