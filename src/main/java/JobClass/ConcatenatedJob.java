package JobClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import ProjectParameter.ParameterClass;
import SubTaskClass.ReadCsvFile;
import SubTaskClass.WriteAppendCsv;
import TaskClass.FilterClass;
import TaskClass.GroupByClass;
import TaskClass.OrderByClass;

import TaskClass.ReportClass;
import TaskClass.SelectClass;
import TaskClass.ShowClass;



public class ConcatenatedJob {
	Dataset<Row> df;
	String url;
	Statement stm;
	SparkSession sc;
	public ConcatenatedJob(SparkSession sc) throws ClassNotFoundException, SQLException{
		this.sc=sc;
	}

	public Dataset<Row> execute(String task) throws SQLException, ClassNotFoundException, IOException {
		int i=0;
		String tasks[]=task.split("->");
		while(i<tasks.length){
			String[] s=tasks[i].split(" ");
			//System.out.println(s[0]+s[1]);
			if(s[0].trim().equals("read")){
				ReadCsvFile rd=new ReadCsvFile(sc);
				String path="C://app//svayam//"+s[1];
				df=rd.read(path);
				
			}
			else if(s[0].trim().equals("write")){
				WriteAppendCsv wc=new WriteAppendCsv();
				String path="C://app//svayam//"+s[1];
				wc.write(df,path);
				df=null;
				
				
			}
			else if(s[0].trim().equals("select")){
				SelectClass scc=new SelectClass(sc);
				df=scc.select(df,s[1]);
			}
			else if(s[0].trim().equals("show")){
				ShowClass shc=new ShowClass(sc);
				df=shc.showDataFrame(df);
			}
			else if(s[0].trim().equals("filter")){
				FilterClass fc=new FilterClass(sc);
				df=fc.filter(df, s[1]);
			}
			else if(s[0].trim().equals("orderBy")){
				OrderByClass obc=new OrderByClass(sc);
				df.collect();
				df=obc.order(df, s[1]);
				df.collect();
			}
			else if(s[0].trim().equals("reportgen")){
				ReportClass rc=new ReportClass(sc);
				df.cache();
				String path="C://app//svayam//"+s[2];
				rc.generate(df,s[1],path);
			}
			else if(s[0].trim().equals("groupBy")){
				GroupByClass gbc=new GroupByClass(sc);
				df=gbc.group(df, s[1]);
				df.collect();
			}
			i++;
		}
		
		return df;
		
		
		
	}


}
