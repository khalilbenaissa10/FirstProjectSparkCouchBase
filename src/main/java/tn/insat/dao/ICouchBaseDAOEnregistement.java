package tn.insat.dao;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public interface ICouchBaseDAOEnregistement {

	
	 DataFrame retrieveDataFrame(SQLContext sql);
}
