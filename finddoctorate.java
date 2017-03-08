import java.io.*;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class finddoctorate {
	public static class MapClass extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split(",");
			if (str[1].equals(" Doctorate degree(PhD EdD)")) {

				context.write(new Text(str[0]),
						new LongWritable(1));
			}
		}
	}

	public static class ReduceClass extends
			Reducer<Text, LongWritable, Text,Text> {

		// private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> value,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			long max = 0;
			for (LongWritable val : value) {

				sum += val.get();
			}
			context.write(new Text("waraniketh"),new Text(key.toString()+","+String.valueOf(sum)));
			//context.write(key, new LongWritable(max));
		}

	}

	public static class ReduceClass1 extends
			Reducer<Text,Text,Text, LongWritable> {
           TreeMap map =new TreeMap();
		// private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			long max = 0;
			for (Text val : value) {
             String[] str=val.toString().split(",");
             map.put(Long.parseLong(str[1]),key);
		}
context.write(key,new LongWritable((long)map.descendingKeySet().first()));
	}
	}
	public static void main(String[] args) throws Exception

	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "agewise");
		job.setJarByClass(finddoctorate.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
