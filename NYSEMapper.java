import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NYSEMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
		//Text stock_sym = new Text();
		//FloatWritable percent = new FloatWritable();

	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] record = value.toString().split(",");
			
			String stockSymbol = record[1];
			float highVal = Float.valueOf(record[4]);
			float lowVal = Float.valueOf(record[5]);
			float percentChange = ((highVal - lowVal) * 100) / lowVal;
			//stock_sym.set(stockSymbol);
			//percent.set(percentChange);
			//context.write(stock_sym, percent);
			context.write(new Text(stockSymbol), new FloatWritable(percentChange));
			
		} catch (IndexOutOfBoundsException e) {
		} catch (ArithmeticException e1) {
		}
	}
}