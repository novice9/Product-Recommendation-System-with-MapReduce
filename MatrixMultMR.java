import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultMR {
	public static class CoOccurMatrixMapper 
			extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				return;
			}
			context.write(new Text(tuple[0]), new Text(tuple[1]));
	
		}
	}
	
	public static class RateMatrixMapper 
			extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String [] tuple = value.toString().trim().split(",");
			if (tuple.length != 3) {
				return;
			}
			context.write(new Text(tuple[1]), new Text(tuple[0] + ":" + tuple[2]));
		}
	}

	public static class MatrixMultReducer
			extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			List<String> relations = new ArrayList<String>();
			List<String> userRates = new ArrayList<String>();
			for (Text value : values) {
				String cell = value.toString().trim();
				if (cell.contains("=")) {
					relations.add(cell);
				} else {
					userRates.add(cell);
				}
			}
			for (String userRate : userRates) {
				String user = userRate.split(":")[0];
				double base = Double.parseDouble(userRate.split(":")[1]);
				for (String relation : relations) {
					String item = relation.split("=")[0];
					String fact = relation.split("=")[1];
					double mult = Double.parseDouble(fact.split("/")[0]) / Double.parseDouble(fact.split("/")[1]);
					context.write(new Text(user + ":" + item), new Text(Double.toString(mult * base)));
				}
			}
		}
	}

	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "MatrixMultMR");
		job.setJarByClass(MatrixMultMR.class);
		
		job.setReducerClass(MatrixMultReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RateMatrixMapper.class);
		
		Path outputPath = new Path(args[2]);
		
		FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
        if (hdfsBuild.exists(outputPath))
        	hdfsBuild.delete(outputPath, true);
        
        TextOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
