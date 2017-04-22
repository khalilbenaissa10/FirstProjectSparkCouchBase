package tn.insat.KMeans;


import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.DataFrame;


public class KmeansAlgorithm implements IKMeansAlgorithm {
	
	public DataFrame TransformToAssembler(DataFrame df){
		 VectorAssembler assembler = new VectorAssembler();
		    assembler
		    .setInputCols(new String[]{"bp", "hr", "rr","spo2", "temp"})
		    .setOutputCol("features");
		    return assembler.transform(df);
	}
	
	public KMeans setKForKMeans(int k){
		return new KMeans().setK(k);
	}
	
	public KMeansModel applyAlgorithmKmeans(KMeans kmeans,DataFrame output){
		return kmeans.fit(output);
	}

}
