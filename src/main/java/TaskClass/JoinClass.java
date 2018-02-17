package TaskClass;


import org.apache.spark.sql.Column;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinClass {
	SparkSession sc;
	public JoinClass(SparkSession sc) {
		// TODO Auto-generated constructor stub
		this.sc=sc;
	}

	public Dataset<Row> join(Dataset<Row> df1, Dataset<Row> df2, String s) {
		
		String[] key=s.split("&");
		System.out.println(key.length+" "+key[0]);
		String col1[]=new String[key.length];
		String col2[]=new String[key.length];
		for(int i=0;i<key.length;i++){
			String[] parts=key[i].split("=");
			System.out.println(parts.length+" "+parts[1]+" "+parts[0]);
			col1[i]=parts[0];
			col2[i]=parts[1];
			
		}
		
		Column jnCol=null;
		for(int i=0;i<col1.length;i++){
			if(i==0){
				jnCol=df1.col(col1[0]).equalTo(df2.col(col2[0]));
			}else{
				jnCol=jnCol.and(df1.col(col1[i]).equalTo(df2.col(col2[i])));
			}
				
		}
		df1=df1.join(df2,jnCol, "inner").drop(df2.col(col2[0]));
		for(int i=0;i<col2.length;i++)
			df1=df1.drop(df2.col(col2[i]));
		
		return df1;
	}

}
