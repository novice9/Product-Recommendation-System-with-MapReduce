import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NormCoOccurMR {
	
	public static class NormCoOccurMapper 
			extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				return;
			}
			String itemA = tuple[0].split(":")[0];
			String itemB = tuple[0].split(":")[1];
			context.write(new Text(itemA), new Text(itemB + "=" + tuple[1]));
		}
	}
	
	public static class NormCoOccurReducer
			extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
			int sum = 0;
			Map<String, Integer> book = new HashMap<String, Integer>();
			for (Text value : values) {
				String [] tuple = value.toString().trim().split("=");
				if (tuple.length != 2) {
					continue;
				}
				sum += Integer.parseInt(tuple[1]);
				book.put(tuple[0], Integer.parseInt(tuple[1]));
			}
			for(Map.Entry<String, Integer> entry : book.entrySet()) { 
				context.write(new Text(entry.getKey()),
						new Text(key.toString() + "=" + entry.getValue() + "/" + Integer.toString(sum)));
			}
		}
	}

	public static void main(String args[]) 
				throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "NormCoOccurMR");
		job.setJarByClass(NormCoOccurMR.class);
		
		job.setMapperClass(NormCoOccurMapper.class);
		job.setReducerClass(NormCoOccurReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
        if (hdfsBuild.exists(outputPath))
        	hdfsBuild.delete(outputPath, true);
        
        TextInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
