package tn.insat.dao;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CouchBaseEnregistrementDAO implements ICouchBaseDAOEnregistement{

	 public DataFrame retrieveDataFrame(SQLContext sql){
		 return  couchbaseReader(sql.read()).couchbase();
	 }
}
