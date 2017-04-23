/* ************************************************************************************
 * Problem 3 NCDC Weather Data
 * 
 * Problem:
 * 1. how's the largest temperature difference at station USW00094728 on each day in 2013?
 * 
 * 
 * The data format is like:
 * USW00094728,20130101,TMAX,44,,,X,2400
 * USW00094728,20130101,TMIN,-33,,,X,2400
 * USW00094728,20130102,TMAX,6,,,X,2400
 * USW00094728,20130102,TMIN,-56,,,X,2400
 * USW00094728,20130103,TMAX,0,,,X,2400
 * USW00094728,20130103,TMIN,-44,,,X,2400
 * 
 * please refer to http://www.i3s.unice.fr/~jplozi/cmpt732/assignment1.pdf
 * to find the problem details
 * 
 * The mapper will get the temp diff for each days
 * The reducer will do nothing.
 * 
 * Data output format:
 * 
 * 
 * ************************************************************************************/

package tzu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemperatureVariations extends Configured implements Tool  {

	
	// TempMapper class is static and extends Mapper abstract class
	// having four hadoop generics type LongWritable, Text, Text, FloatWritable
	
	public static class TempMapper 
	extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		// This method takes the input as text data type
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	// Converting the each single line to String
			String line = value.toString();
			String[] dataSplit = line.split(",");
			// CENTRAL_PARK
			if (dataSplit[0].trim().equals("USW00094728") && (dataSplit[2].trim().equals("TMAX"))){
        		Text date = new Text(dataSplit[1]);
				float maxTemp = Float.parseFloat(dataSplit[3].trim());
				context.write(date, new FloatWritable(maxTemp));
			}
			
			if (dataSplit[0].trim().equals("USW00094728") && (dataSplit[2].trim().equals("TMIN"))){
				Text date = new Text(dataSplit[1]);
				float minTemp = Float.parseFloat(dataSplit[3].trim());
				context.write(date, new FloatWritable(minTemp));
			}
		}
	}
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TemperatureVariations(), args);
        System.exit(res);
    }
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "temperature change");
        job.setJarByClass(TemperatureVariations.class);
		
        job.setInputFormatClass(TextInputFormat.class);
        
		job.setMapperClass(TempMapper.class);        
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;	
	}
}