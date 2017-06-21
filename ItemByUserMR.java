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

public class ItemByUserMR {
	
	public static class ItemByUserMapper 
			extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			
			String [] tuple = value.toString().trim().split(",");
			if (tuple.length != 3) {
				return;
			}
			context.write(new Text(tuple[0]), new Text(tuple[1]));
		}
	}
	
	public static class ItemByUserReducer
			extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString() + ",");
			}
			sb.deleteCharAt(sb.length() - 1);
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String args[]) 
				throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "ItemByUserMR");
		job.setJarByClass(ItemByUserMR.class);
		
		job.setMapperClass(ItemByUserMapper.class);
		job.setReducerClass(ItemByUserReducer.class);
		
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
