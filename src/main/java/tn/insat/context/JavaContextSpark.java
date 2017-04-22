package tn.insat.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaContextSpark {
	
	SparkConf conf1;
	SparkConf conf2 ;
	SparkConf conf3 ;
	JavaSparkContext sc;
	public JavaSparkContext returnContextPatient(){
	
		if(conf1==null){
		conf1 = new SparkConf()
		    .setAppName("FirstProjectSparkCouchBase")
		    .setMaster("local[*]")
		    .set("com.couchbase.bucket.PatientBucket", "khalil22307246");
		}

		if(sc ==null){
		 sc = new JavaSparkContext(conf1);
		}
		return sc;
	}
	
	public JavaSparkContext returnContextEnregistrement(){

		
		if(conf2==null){
		conf2 = new SparkConf()
		    .setAppName("FirstProjectSparkCouchBase")
		    .setMaster("local[*]")
		    .set("com.couchbase.bucket.EnregistrementsBucket", "khalil22307246");
		}

		if(sc ==null){
		 sc = new JavaSparkContext(conf2);
		}
		return sc;
	}
	
public JavaSparkContext returnContextMongoDb(){

		
		if(conf3==null){
		conf3 = new SparkConf()
				.setMaster("local")
		        .setAppName("MongoSparkConnectorTour")
		        .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
		        .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection");
		}

		if(sc ==null){
		 sc = new JavaSparkContext(conf3);
		}
		return sc;
	}
	
	
}
