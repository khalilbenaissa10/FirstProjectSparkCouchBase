package tn.insat.KMeans;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.DataFrame;

public interface IKMeansAlgorithm {

	DataFrame TransformToAssembler(DataFrame df);
	KMeans setKForKMeans(int k);
	KMeansModel applyAlgorithmKmeans(KMeans kmeans,DataFrame output);
}
