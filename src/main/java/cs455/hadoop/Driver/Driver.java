package cs455.hadoop.Driver;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class Driver {

	// Output KEY(ArtistID TAB ArtistName) VALUE(1)
	public static class Question1MetadataMapper extends Mapper<Object, Text, Text, IntWritable>{
		private static final IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(!value.toString().isEmpty()){
				String[] input = value.toString().split("\\s+");
				if(input.length!=14) return;
				String artistID = input[2].trim();
				String artistName = input[6].trim();
				context.write(new Text(artistID+"\t"+artistName), one);
			}
		}
	}
	// Output KEY(ArtistID TAB ArtistName) VALUE(numberOfSongs)
	public static class Question1ArtistReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private static Text maxArtist = null;
		private static int maxNum = 0;
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int numberSongs = 0;
			for(IntWritable num:values) {
				numberSongs+=num.get();
			}
			if(maxArtist==null) {
				maxArtist = key;
				maxNum = numberSongs;
			}else {
				if(maxNum<numberSongs) {
					maxArtist = key;
					maxNum = numberSongs;
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(maxArtist, new IntWritable(maxNum));
		}
	}
	
	// Output KEY(ArtistID TAB ArtistName) VALUE(max loudness)
		public static class Question2AnalysisMapper extends Mapper<Object, Text, Text, DoubleWritable>{
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				if(!value.toString().isEmpty()){
					String[] input = value.toString().split("\\s+");
					if(input.length!=14) return;
					String artistID = input[2].trim();
					String artistName = input[6].trim();
					double max = Double.parseDouble(input[21].trim());
					// Don't use combiner, needs individual max loudness for reducer
					context.write(new Text(artistID+"\t"+artistName), new DoubleWritable(max));
				}
			}
		}
		// Output KEY(ArtistID TAB ArtistName) VALUE(Average max Loudness)
		public static class Question2ArtistReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
			private static Text maxArtist = null;
			private static double maxNum = 0;
			public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
				int numberSongs = 0;
				double sumLoudness = 0;
				for(DoubleWritable num:values) {
					numberSongs++;
					sumLoudness+=num.get();
				}
				double averageLoudness = sumLoudness/numberSongs;
				if(maxArtist==null) {
					maxArtist = key;
					maxNum = numberSongs;
				}else {
					if(maxNum<averageLoudness) {
						maxArtist = key;
						maxNum = averageLoudness;
					}
				}
			}
			
			public void cleanup(Context context) throws IOException, InterruptedException {
				context.write(maxArtist, new DoubleWritable(maxNum));
			}
		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question1Job");
		
		job.setJarByClass(Driver.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setMapperClass(Question1MetadataMapper.class);
		job.setReducerClass(Question1ArtistReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
			
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("Q1"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}