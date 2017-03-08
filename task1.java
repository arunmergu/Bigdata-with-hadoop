import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//import org.w3c.dom.Text;

public class task1 {
	
	
	 public static class MapClass
     extends Mapper<LongWritable, Text, Text,IntWritable>
{
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {

  	String[] str = value.toString().split(",");
  	context.write(new Text("all"),new IntWritable(1));
  	//if(str[7].equals(" United-States")){
  	//context.write(new Text(" United-States"),new IntWritable(1));
  	//}
  	if(!str[7].equals(" United-States")){
  	  	context.write(new Text("Non-United-States"),new IntWritable(1));
  	  	}
  	  	
  	
  }
}
	 
	 public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	    {
	    	private LongWritable result = new LongWritable();
	    	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	    	{int count=0;int count1=0;
	    	for(IntWritable val:values){
	    		
	    	count=count+val.get();	
	    		
	    	}
	    	
	    	
	    	
	    	
	    	context.write(key,new IntWritable(count));
	    	
	    
	    	
	    	}
	    	}

	  public static void main(String[] args) throws Exception 
	  
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"immigration");
	    job.setJarByClass(task1.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

}
