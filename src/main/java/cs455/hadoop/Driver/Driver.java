package cs455.hadoop.Driver;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	// Output KEY(UniqueID) VALUE(information)
	public static class AnalysisMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(!value.toString().isEmpty()){
				String[] input = value.toString().split("\\s+");
				if(input.length!=31) return;
				String uniqueID = input[0].trim();
				context.write(new Text(uniqueID), value);
			}
		}
	}

	// Output KEY(UniqueId) VALUE(information)
	public static class MetadataMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(!value.toString().isEmpty()){
				String[] input = value.toString().split("\\s+");
				if(input.length!=14) return;
				String uniqueID = input[7].trim();
				context.write(new Text(uniqueID), value);
			}
		}
	}
	// Output KEY(DocId) VALUE(Word TAB TF)
	public static class SongReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CombinerJob");
		
		job.setJarByClass(Driver.class);
		job.setReducerClass(SongReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);	
		
		MultipleInputs.addInputPath(job, new Path("analysis*.csv"),
			    TextInputFormat.class, AnalysisMapper.class);
			MultipleInputs.addInputPath(job, new Path("metadata*.csv"),
			    TextInputFormat.class, MetadataMapper.class); 
		FileOutputFormat.setOutputPath(job, new Path("combined"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}