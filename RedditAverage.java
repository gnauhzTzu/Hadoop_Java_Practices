/* ************************************************************************************
 * 
 * Find the average scores of reddit comments
 * 1. Parse the json file of reddit comments as structured data using 
 *    Jackson JSON parser (other tools includes Gson, Genson, FlexJson etc.)
 * 2. The mapper output will be like pair(subreddit,  pair(count, total score)).
 * 3. The combiner will be wrote together with mapper and reducer to summarise the comments count/score
 *    for each subreddit
 * 
 * Data used in this project can be downloaded at:
 * https://www.kaggle.com/reddit/reddit-comments-may-2015
 * 
 * Data format:
 * data file was compressed as .gz
 * the input to our mapper will be lines (from TextInputFormat) of JSON-encoded data
 * 
 * How to run:
 * this file need to compile together with LongPairWritable and MultiLineJSONInputFormat
 * compile to a jar file as usual, make sure the input data is avaliable in hdfs
 * the hadoop external jars should be first set in hadoop class path by:
 * export HADOOP_CLASSPATH=/path/to/jar1.jar:/path/to/jar2.jar
 * then submit hadoop the job by:
 * yarn jar myProject.jar tzu.RedditAverage -libjars /path/to/jar1.jar,/path/to/jar2.jar inputfiles outputfiles
 * 
 * ************************************************************************************/

package tzu;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {
	
	public static class RedditMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
		
		// mapper read in json (one-line) file, paser it and get subreddit, score
		// creat a LongPairWritable pair: (subreddit, (comments count, score)) 
		// each score in the data is bind with 1 comment count
		private LongPairWritable comment_score = new LongPairWritable();
		
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	ObjectMapper json_mapper = new ObjectMapper();
        	 
        	JsonNode data = json_mapper.readValue(value.toString(), JsonNode.class);
        	
        	Text subreddit = new Text(data.get("subreddit").textValue());
        	long score = data.get("score").longValue();
        	
        	comment_score.set(1, score);
        	
        	context.write(subreddit, comment_score);
        }
    }
	
	// combiner: sum comments count and scores by subreddit
	// save effort for reducer
	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {	
		private LongPairWritable comment_score = new LongPairWritable();
		
		@Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			
			long sumCommentsCount = 0;
			long sumScore = 0;
            for (LongPairWritable val : values) {
            	sumCommentsCount += val.get_0();
            	sumScore += val.get_1();
            }
            comment_score.set(sumCommentsCount, sumScore);
            context.write(key, comment_score);
        }
	}
	
	// reducer: calculate the average score for each subreddit
	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		private double averageScore = 0;
		
		@Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			
			long sumCommentsCount = 0;
			long sumScore = 0;
            for (LongPairWritable val : values) {
            	sumCommentsCount += val.get_0();
            	sumScore += val.get_1();
            }
            
            averageScore = sumScore/sumCommentsCount;
            
            result.set(averageScore);
            context.write(key, result);
        }
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
	}
	
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "reddit average");
        job.setJarByClass(RedditAverage.class);
 
        // If the input is one-line json, use it
        // job.setInputFormatClass(TextInputFormat.class); 
        // If the input is the json with multiple lines, use it
        job.setInputFormatClass(MultiLineJSONInputFormat.class);
        
        job.setMapperClass(RedditMapper.class);
        job.setCombinerClass(RedditCombiner.class);
        job.setReducerClass(RedditReducer.class);
 
        // the output of mapper should be LongPairWritable
        job.setMapOutputValueClass(LongPairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
               
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}