package SubTaskClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import ProjectParameter.ParameterClass;

public class ReadCsvFile {
	SparkSession spark;
	
	public ReadCsvFile(SparkSession spark) throws ClassNotFoundException, SQLException {
		this.spark=spark;
	}

	public Dataset<Row> read(String path) throws SQLException {
		Dataset<Row> df=null;
		SQLContext sqlContext= spark.sqlContext();
		//List<StructField> fields = new ArrayList<StructField>();
		//fields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
		//fields.add(DataTypes.createStructField("language", DataTypes.StringType, true));
		//fields.add(DataTypes.createStructField("user_id", DataTypes.LongType, true));
		//StructType schema = DataTypes.createStructType(fields);
		df=sqlContext.read().format("csv").option("header",true).option("mode", "PERMISSIVE")
		.load(path);
		if(df==null){
			
			System.out.println("INVALID_FILE");
			return null;
		
		}
		else
		return df;
	}

}

