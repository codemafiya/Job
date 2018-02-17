package JobClass;

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Stack;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import SubTaskClass.ReadCsvFile;
import SubTaskClass.WriteAppendCsv;
import TaskClass.FilterClass;
import TaskClass.GroupByClass;
import TaskClass.JoinClass;
import TaskClass.OrderByClass;
import TaskClass.ReportClass;
import TaskClass.SelectClass;
import TaskClass.ShowClass;
public class TaskExecuter {

	Stack<Dataset<Row>> df;
	SparkSession sc;
	public TaskExecuter(int total, SparkSession sc) {
		this.sc=sc;
		df=new Stack<Dataset<Row>>();
	}
	public void execute(String task, int i) throws ClassNotFoundException, SQLException, IOException {
		System.out.println(task+"  "+i);
		if(task.contains("->")){
			ConcatenatedJob cj=new ConcatenatedJob(sc);
			
			df.push(cj.execute(task));
		}
		else{
			String args[]=task.split(" ");
			if(args[0].trim().equals("select")){
				SelectClass scc=new SelectClass(sc);
				Dataset<Row> temp=df.pop();
				df.push(scc.select(temp, args[1]));
			}
			else if(args[0].trim().equals("read")){
				ReadCsvFile rd=new ReadCsvFile(sc);
				String path="C://app//svayam//"+args[1];
				df.push(rd.read(path));
				
			}
			else if(args[0].trim().equals("write")){
				WriteAppendCsv wc=new WriteAppendCsv();
				String path="C://app//svayam//"+args[1];
				wc.write(df.pop(),path);
				df=null;
				
				
			}
			else if(args[0].trim().equals("show")){
				ShowClass shc=new ShowClass(sc);
				Dataset<Row> temp=df.pop();
				df.push(shc.showDataFrame(temp));
				
			}
			else if(args[0].trim().equals("filter")){
				FilterClass fc=new FilterClass(sc);
				Dataset<Row> temp=df.pop();
				df.push(fc.filter(temp, args[1]));
				
				
			}
			else if(args[0].trim().equals("orderBy")){
				OrderByClass obc=new OrderByClass(sc);
				Dataset<Row> temp=df.pop();
				df.push(obc.order(temp,args[1]));
				
			}
			else if(args[0].trim().equals("groupBy")){
				GroupByClass gbc=new GroupByClass(sc);
				Dataset<Row> temp=df.pop();
				df.push(gbc.group(temp, args[1]));
				
				
			}
			else if(args[0].trim().equals("join")){
				JoinClass jc=new JoinClass(sc);
				Dataset<Row> temp=df.pop();
				Dataset<Row> temp1=df.pop();
				df.push(jc.join(temp1,temp,args[1]));
				//df[i].show();
				
			}
			else if(args[0].trim().equals("reportgen")){
				ReportClass rc=new ReportClass(sc);
				Dataset<Row> temp=df.pop();
				String path="C://app//svayam//"+args[2];
				temp.cache();
				rc.generate(temp,args[1],path);
				
			}
			else
				System.out.println("InValid Job");
		}
		
	}
	
}
