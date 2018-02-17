package JobClass;
import org.apache.spark.sql.SparkSession;

public class TaskSplit {
	String job;
	SparkSession sc;
	public TaskSplit(String job, SparkSession sc) {
		this.job=job;
		this.sc=sc;
	}
	
	public String getTask(int i, int j) {
		int count=0;
		int start=-1;
		int end=-1;
		for(int k=i+1;k<j;k++){
			if(job.charAt(k)=='(')
				count++;
			else if(job.charAt(k)==')')
				count--;
			else if(count==0&&start==-1)
				start=k;
			else if(count==0&&start!=-1)
				end=k;
		}
		return job.substring(start, end+1); 		
	}
}
