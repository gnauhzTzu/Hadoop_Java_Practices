/* ****************************************************************************
 * 
 * 1. Use LongSumReducer instead of IntWritable in combiner and reducer
 *    in compatible with big integer
 * 2. Use java.text.Normalizer to resolve the complicated unicode
 * 3. Use BreakIterator instead of StringTokenizer to return word breaks for defautl locale
 * 
 * ***************************************************************************/
package tzu;

import java.io.IOException;
import java.util.Locale;
import java.text.BreakIterator;
import java.text.Normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WordCountImproved extends Configured implements Tool {
 
	/**
	 * @param args
	 */
	
    public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
    	// keep count in LongWritable format to accommodate larger number (64 bit)
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	//return a break iterator for word breaks for locale
        	BreakIterator breakiter = 
        			BreakIterator.getWordInstance(new Locale ("en","US"));
        	
        	int start = breakiter.first();
        	// convert to lower case
        	String line = value.toString().toLowerCase();
        	breakiter.setText(line);
        	for (int end = breakiter.next();
        			end != BreakIterator.DONE;
        			start = end, end = breakiter.next()) {
        		// substring and trim off white spaces
        		word = new Text(Normalizer.normalize(line.substring(start,end).trim(), 
						Normalizer.Form.NFD));
        		// convert back to string to get length
        		if ( word.toString().length() > 0 ) {       			
        			context.write(word, one);       			
        		}
        	}
        }
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count improved");
        job.setJarByClass(WordCountImproved.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

