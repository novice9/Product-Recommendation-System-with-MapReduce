import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ItemRelationMR {
	public static class ItemRelationMapper 
			extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] tuple = value.toString().trim().split("\t");
			if (tuple.length != 2) {
				return;
			}
			String [] items = tuple[1].trim().split(",");
			for (int i = 0; i < items.length; ++i) {
				for (int j = 0; j < items.length; ++j) {
					context.write(new Text(items[i] + ":" + items[j]), new IntWritable(1));
				}
			}
		}
	}

	public static class ItemRelationReducer
			extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new Text(Integer.toString(sum)));
		}
	}

	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "ItemRelationMR");
		job.setJarByClass(ItemRelationMR.class);
		
		job.setMapperClass(ItemRelationMapper.class);
		job.setReducerClass(ItemRelationReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
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
