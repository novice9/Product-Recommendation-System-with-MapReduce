import java.io.IOException;
import java.net.URI;

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

public class UnitSumMR {
	
	public static class UnitSumMapper 
			extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				// invalid format or edge case
				return;
			}
			context.write(new Text(tuple[0]), new Text(tuple[1]));
		}
	}
	
	public static class UnitSumReducer
			extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			for (Text value : values) {
				sum += Double.parseDouble(value.toString());
			}
			context.write(key, new Text(Double.toString(sum)));
		}
	}
	
	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "UnitSumMR");
		job.setJarByClass(UnitSumMR.class);
		
		job.setMapperClass(UnitSumMapper.class);
		job.setReducerClass(UnitSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outputPath = new Path(args[1]);
		
		FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
        if (hdfsBuild.exists(outputPath))
        	hdfsBuild.delete(outputPath, true);
        
        TextOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
