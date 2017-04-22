package tn.insat.client;

import org.apache.spark.api.java.JavaSparkContext;

import com.couchbase.spark.japi.CouchbaseSparkContext;

import tn.insat.KMeans.IKMeansAlgorithm;
import tn.insat.KMeans.KmeansAlgorithm;
import  tn.insat.context.*;
import tn.insat.dao.CouchBaseEnregistrementDAO;
import tn.insat.dao.CouchBasePatientDAO;
import tn.insat.dao.ICouchBaseDAOPatient;
import tn.insat.dao.ICouchBaseDAOEnregistement;



import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import com.mongodb.spark.MongoSpark;


public class Client {
	
	public static ICouchBaseDAOPatient dao = new CouchBasePatientDAO();
	public static ICouchBaseDAOEnregistement dao_enr = new CouchBaseEnregistrementDAO();
	static CouchbaseSparkContext csc;

	
	public static void main(String[] args) {
		JavaContextSpark jcon = new JavaContextSpark();
		//JavaSparkContext jsc1 = jcon.returnContextPatient();
		JavaSparkContext jsc2 = jcon.returnContextEnregistrement();
		JavaSparkContext jsc_mdb = jcon.returnContextMongoDb();
		IKMeansAlgorithm algorithm = new KmeansAlgorithm();
		
		 SQLContext sql = new SQLContext(jsc2);
		 
		
		int k = 2 ;
		
		

		//	csc = couchbaseContext(jsc1);
		
			//premiere methode
			//List<JsonDocument> docs = dao.retrieveJsonDocument(csc);
			//System.out.println(docs);
			
			//Deuxieme methode query
			//List<CouchbaseQueryRow> results =dao.retrieveQuery(csc);
			//System.out.println(results);
			
			//troisieme methode
			//JavaRDD<String> ids = dao.retrieveJavaRDD(jsc);
			 //docs = couchbaseRDD(ids).couchbaseGet().collect();
			 //System.out.println(docs);
			 
			 //quatrieme methode
			// JavaRDD<CouchbaseQueryRow> rddquery = dao.retrieveQueryRDD(csc, jsc);
			// System.out.println(rddquery.count());
			 
			// Use SparkSQL from Java
			

			 // Wrap the Reader and create the DataFrame from Couchbase
			DataFrame df = dao_enr.retrieveDataFrame(sql);
			System.out.println(df.count());
			
			
			 df.drop("patientId");
			 df.drop("time");
			// df.show();
			 
			//JavaRDD<Row> add = df.toJavaRDD();
			 
//			 VectorAssembler assembler = new VectorAssembler();
//			    assembler
//			    .setInputCols(new String[]{"META_ID","bloodtype","dob","fName","gender","lName","patientId","title"})
//			    .setOutputCol("features");
//			    DataFrame output = assembler.transform(df);
			    
//			    VectorAssembler assembler = new VectorAssembler();
//			    assembler
//			    .setInputCols(new String[]{"bp", "hr", "rr","spo2", "temp"})
//			    .setOutputCol("features");
//			    DataFrame output = assembler.transform(df);
			 
			 DataFrame output = algorithm.TransformToAssembler(df);
			    
			    System.out.println("apres transformation");
			  //  output.show();
			 
			   //Trains a k-means model
			 KMeans kmeans = algorithm.setKForKMeans(k);
			  KMeansModel model =algorithm.applyAlgorithmKmeans(kmeans, output);


			    // Shows the result
			    Vector[] centers = model.clusterCenters();
			    System.out.println("Cluster Centers: ");
			    for (Vector center: centers) {
			      System.out.println(center);
			    } 
			   
			    double WSSSE = model.computeCost(output);
		        //System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
			    
			    double cost = model.computeCost(output);
			    //System.out.println("Cost: " + cost);
			    
//			    try {
//					model.save("c:/dirassa/gl4/GL4S2/dataMining/resultat");
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			    
			   DataFrame result = model.transform(output);
			  // result.show();
//			   for (Row row :result.collect()) {
//				   System.out.println(row);
//				
//			} 
			   
		
			 DataFrame afterdrop = result.drop("features");
			afterdrop.show();
			 MongoSpark.write(afterdrop).option("spark.mongodb.output.uri", "mongodb://khalil:khalil22307246@ds111461.mlab.com:11461/patientdb").option("collection", "enregistrements").mode("overwrite").save();
			    jsc2.stop();
			 
			 
			   
	}

}
