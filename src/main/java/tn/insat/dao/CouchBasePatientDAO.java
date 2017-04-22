package tn.insat.dao;

import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlQuery;

public class CouchBasePatientDAO implements ICouchBaseDAOPatient {
	
	public List<JsonDocument> retrieveJsonDocument(CouchbaseSparkContext csc){
		return csc
			    .couchbaseGet(Arrays.asList("Patient11223344", "Patient11223355"))
			    .collect();
		
		
	}
	
	public List<CouchbaseQueryRow> retrieveQuery(CouchbaseSparkContext csc){
		return  csc
			    .couchbaseQuery(N1qlQuery.simple("SELECT * FROM `PatientBucket` LIMIT 10"))
			    .collect();
	}

	public JavaRDD<String> retrieveJavaRDD(JavaSparkContext jsc){
		return jsc.parallelize(Arrays.asList("Patient11223344", "Patient11223355", "Patient11223366", "Patient11223377"));
	}



	public JavaRDD<CouchbaseQueryRow> retrieveQueryRDD(CouchbaseSparkContext csc,JavaSparkContext jsc){
		
		
		List<CouchbaseQueryRow> liste =	this.retrieveQuery(csc);
		 return jsc.parallelize(liste);
	}



	public DataFrame retrieveDataFrame(SQLContext sql){
		return  couchbaseReader(sql.read()).couchbase();
	}

	
}
