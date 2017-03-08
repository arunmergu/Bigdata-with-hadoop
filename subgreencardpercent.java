

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
//import org.w3c.dom.Text;

public class subgreencardpercent{
	
	
	 public static class MapClass
     extends Mapper<LongWritable, Text, Text,LongWritable>
{
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {

  	String[] str = value.toString().split("\t");
  	context.write(new Text("all"),new LongWritable(Long.parseLong(str[1])));
  	
  }
}
	 
	 public static class ReduceClass extends Reducer<Text,LongWritable,Text,DoubleWritable>
	    {
	    	private LongWritable result = new LongWritable();
	    	int i=0;
	    	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
	    	{long count=0;long count1=0;
	    	for(LongWritable val:values){	
	    	if(i==0){
	    		count=val.get();
	    		i++;}
	    	else{
	    		count1=val.get();
	    	}	    		
	    	}
	    	double per=(count*100)/(count1+count);
	    	context.write(new Text("greencardpercent"),new DoubleWritable(per));
	    	}
	    	}

	  public static void main(String[] args) throws Exception 
	  
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf,"immigration");
	    job.setJarByClass(subgreencardpercent.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

}

