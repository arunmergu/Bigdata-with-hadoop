import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class maxincome {
	
	public static class MapClass extends Mapper<LongWritable,Text,DoubleWritable,DoubleWritable> 
	{
	
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String[] str = value.toString().split(",");
			DoubleWritable result = new DoubleWritable();
			DoubleWritable result1 = new DoubleWritable();
			//Text result1 = new Text();
			result1.set(Double.parseDouble(str[0]));
			result.set(Double.parseDouble(str[5]));
			context.write(result1,result);
		}
	}
	public static class ReducerClass extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable>
	{
		public void reduce(DoubleWritable key,Iterable<DoubleWritable> value,Context context) throws IOException,InterruptedException
		{
			
			double sum=0;
			for(DoubleWritable val:value)
			{
				sum = sum + val.get();
			}
		
		context.write(key, new DoubleWritable(sum));
		}
		
		
		
	}
	 public static void main(String[] args) throws Exception 
	  
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"agewise");
	    job.setJarByClass(maxincome.class);
	    job.setMapperClass(MapClass.class);
	   // job.setCombinerClass(CombinerClass.class);
	    job.setReducerClass(ReducerClass.class);
	    job.setMapOutputKeyClass(DoubleWritable.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	


}




















