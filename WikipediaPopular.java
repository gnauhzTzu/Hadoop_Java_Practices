/* **************************************************************************************
 * 
 * Find the most-viewed wikipedia pages
 * 1. filter out the English pages 
 * 2. filter out the title is "Main_Page" and title start with "Special:"
 * 3. the data was stored as files with name on each file represent the year-month-day-hour
 * 
 * data used in this project can be downloaded at:
 * https://dumps.wikimedia.org/other/pagecounts-raw/2016/2016-01/
 * 
 * data format:
 * "the page language" "title" "requested times" "bytes"
 * columns are separated by one blank space
 * 
 * how to run:
 * the code was developed in eclipse mars, you can either config eclipse hadoop plugin
 * or follow the tutorial here:
 * https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * two arguments should be provided together:
 * @arg0: the directory in hdfs contains the data files
 * @arg1: the file path that you want to output the result
 * 
 * *************************************************************************************/
package tzu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {
	
	public static class WikiMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
        		) throws IOException, InterruptedException {
        	
        	// get the file path and the input files names
        	Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        	String fileNameString = filePath.toString();

            String line = value.toString();
            
            // split the rows in data
            String [] dataSplit = line.split("\\s+");
            
            if (dataSplit[0].equals("en") 
            		&& !dataSplit[1].contains("Main_Page") 
            		&& !dataSplit[1].startsWith("Special:")) {
            	
            		LongWritable pageCount = new LongWritable(Long.parseLong(dataSplit[2]));            		
                	
                	// extract the time from the files names
                	// as file name was like pagecounts-20141201-130000.gz 
            		// so the 11 to 21 char are what we need
                    String fileName = fileNameString.substring(11, 22);
                                                  
            		context.write(new Text(fileName), pageCount);
            }
        }
    }
	
	public static class WikiReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context
        		) throws IOException, InterruptedException {
            long max = 0;
            for (LongWritable val : values) {
                if (val.get() > max){
                	max = val.get();
                }
            }
            result.set(max);
            context.write(key, result);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
	}
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Wiki");
        job.setJarByClass(WikipediaPopular.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(WikiMapper.class);
        job.setCombinerClass(WikiReducer.class);
        job.setReducerClass(WikiReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}