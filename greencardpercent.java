import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.examples.SecondarySort.Reduce;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Reducer;


public class greencardpercent {
	
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
		  // String count;
			String[] str = value.toString().split(",");
			
			context.write(new Text("all"),new LongWritable(1));
			
			if (!str[7].equals(" United-States")
					&& str[8]
							.equals(" Foreign born- U S citizen by naturalization"))
			{
				

			
			context.write(new Text("non-unitedstates"), new LongWritable(1));
		}}}
		public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
		{
			
			public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException, InterruptedException
			{
				long count =0;long count1=0;
				for(LongWritable val:value)
					
				{
					count = count+val.get();
			
				}
				context.write(key, new LongWritable(count) );
			}

		
	}
	 public static void main(String[] args) throws Exception 
	  
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"immigration");
	    job.setJarByClass(greencardpercent.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(IntSumReducer.class);
	   job.setReducerClass(ReduceClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

}



