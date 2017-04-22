package tn.insat.dao;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.spark.japi.CouchbaseSparkContext;
import com.couchbase.spark.rdd.CouchbaseQueryRow;

public interface ICouchBaseDAOPatient {
	 List<JsonDocument> retrieveJsonDocument(CouchbaseSparkContext csc);
	 List<CouchbaseQueryRow> retrieveQuery(CouchbaseSparkContext csc);
	 JavaRDD<String> retrieveJavaRDD(JavaSparkContext jsc);
	 JavaRDD<CouchbaseQueryRow> retrieveQueryRDD(CouchbaseSparkContext csc,JavaSparkContext jsc);
	 DataFrame retrieveDataFrame(SQLContext sql);
	

}
