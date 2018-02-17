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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import ProjectParameter.ParameterClass;

public class WriteAppendCsv {

	public WriteAppendCsv() throws ClassNotFoundException, SQLException {

	}

	public int write(Dataset<Row> df, String path) throws SQLException {
		
		df.write().format("com.databricks.spark.csv").option("header", "true").mode(SaveMode.Overwrite).save(path);
		return 0;
	}

}
