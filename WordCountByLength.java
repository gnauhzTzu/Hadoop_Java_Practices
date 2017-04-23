/* ********************************************************************************
 * Like the other two wordcount script, input a txt file
 * count the number of words having 1 letter, having 2 letters, and so on
 * 
 * *******************************************************************************/

package tzu;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountByLength extends Configured implements Tool {
	 
	/**
	 * @param args
	 */
	
    public static class LengthMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text("");
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", " ");
        	StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
        		if ( word.toString().length() > 0 ) {
        			Text letterCount = new Text(String.valueOf(word.toString().length()));
        			context.write(letterCount, one);       			
        		}
            }
        }
    }
 
    public static class LengthReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
 
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count length");
        job.setJarByClass(WordCount.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(LengthMapper.class);
        job.setCombinerClass(LengthReducer.class);
        job.setReducerClass(LengthReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
