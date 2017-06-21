import java.io.IOException;

public class Driver {
	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {
		String rawInput = new String(args[0]);
		String itemDividedByUser = new String("hdfs://hadoop-master:9000/itemByUser");
		String relationBetweenItems = new String("hdfs://hadoop-master:9000/itemRelation");
		String coOccurMatrix = new String("hdfs://hadoop-master:9000/coOccurMatrix");
		String multMatrixCell = new String("hdfs://hadoop-master:9000/multMatrixCell");
		String itemRecommend = new String(args[1]);
		
		String [] paras1 = {rawInput, itemDividedByUser};
		String [] paras2 = {itemDividedByUser, relationBetweenItems};
		String [] paras3 = {relationBetweenItems, coOccurMatrix};
		String [] paras4 = {coOccurMatrix, rawInput, multMatrixCell};
		String [] paras5 = {multMatrixCell, itemRecommend};
		
 		ItemByUserMR.main(paras1);
 		ItemRelationMR.main(paras2);
 		NormCoOccurMR.main(paras3);
 		MatrixMultMR.main(paras4);
 		UnitSumMR.main(paras5);
	}
}
