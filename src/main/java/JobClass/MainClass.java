package JobClass;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.Stack;

import org.apache.spark.sql.SparkSession;

public class MainClass {
	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException{
		Scanner sc=new Scanner(System.in);
		String job=sc.nextLine();
		int i=0,count=0;
		String s[]=job.split(": ");
		if(s[0].trim().equals("job")){
			SparkSession spark = SparkSession
				      .builder()
				      .master("local[5]")
				      .appName("ProjectApp")
				      .getOrCreate();
			job=s[1].trim();
			for(i=0;i<job.length();i++){
				if(job.charAt(i)=='(')
					count++;
			}
			int[][] range= new int[count][2];
			int k=0;
			Stack<Integer> st=new  Stack<Integer>();
			int j;
			for(i=0;i<job.length();i++){
				if(job.charAt(i)=='(')
					st.push(i);
				else if(job.charAt(i)==')'){
					j=st.pop();
					range[k][0]=j;
					range[k][1]=i;
					k++;
					
				}
			}
			TaskSplit ts=new TaskSplit(job,spark);
			TaskExecuter te=new TaskExecuter(count,spark);
			for(i=0;i<count;i++){
				String task=ts.getTask(range[i][0],range[i][1]);
				//System.out.println(task);
				te.execute(task,i);
			}
			spark.close();
		}
		else if(s[0].trim().equalsIgnoreCase("complexjob")){
			ComplexToSimple cts=new ComplexToSimple();
			System.out.println(s[1]);
			job=cts.getJob(s[1].trim());
			SparkSession spark = SparkSession
				      .builder()
				      .master("Local[5]")
				      .appName("ProjectApp")
				      .getOrCreate();
			for(i=0;i<job.length();i++){
				if(job.charAt(i)=='(')
					count++;
			}
			int[][] range= new int[count][2];
			int k=0;
			Stack<Integer> st=new  Stack<Integer>();
			int j;
			for(i=0;i<job.length();i++){
				if(job.charAt(i)=='(')
					st.push(i);
				else if(job.charAt(i)==')'){
					j=st.pop();
					range[k][0]=j;
					range[k][1]=i;
					k++;
				}
			}
			TaskSplit ts=new TaskSplit(job,spark);
			TaskExecuter te=new TaskExecuter(count,spark);
			for(i=0;i<count;i++){
				String task=ts.getTask(range[i][0],range[i][1]);
				te.execute(task,i);
			}
			spark.close();
		}
		else
			System.out.println("Invalid Job");
	}

}
