/* ************************************************************************************
 * 
 * Estimate Euler's consant using a stochastic representation
 * 1 . Generate random numbers in [0,1] until the total is more than 1: 
 *     the number of random values we needed for that is an estimate of e. 
 *     If we average over say 10000 iterations, we will get a value really close to e.
 * 
 * 2. For each iteration, save the number when we get the total greater than 1,
 *    each number per line
 * 3. In mapper, repeat the iterations to get (iteration number, value)
 * 4. Use hadoop counters: shared variables that hold integers which can only be incremented
 * 5. Check the two haddop counter in the output, this file would be "Euler count", "Euler iterations"
 *    The euler is estimated by count/iterations
 * 
 * 
 * The goal of this project is to evaluate the calculation of hadoop when the data is not big
 * 
 * How to run:
 * compile to a jar file as usual, make sure the input data is avaliable in hdfs
 * then submit hadoop the job by:
 * yarn jar myProject.jar tzu.EulerEstimator inputfiles outputfiles
 * 
 * Note:
 * two sets of input files have been tested in this project. 
 * first one is a single file with 100 lines, number is 300000 per each line
 * second one is 100 files with 1 line in each file, number is also 300000 in that line.
 * 
 * it appears running with first one is faster than second one because of the I/O of 100 files
 * first one takes 93938ms to finish, and
 * GC time elapsed (ms)=859
 * CPU time spent (ms)=249770
 * second one takes 274906ms to finish, and
 * GC time elapsed (ms)=23848
 * CPU time spent (ms)=402200
 * 
 * 
 * ************************************************************************************/ 

package tzu;

import java.io.IOException;
import java.util.Random;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EulerEstimator extends Configured implements Tool {
 
    public static class EulerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	// get file's hashcode plus the unique key in the mapper to make sure the 
        	// random seed has high probability to be unique for each iteration
        	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        	long hashCode = fileName.hashCode();
        	Random randomSeed = new Random();
        	randomSeed.setSeed(hashCode + key.get());
        	
        	// get interger from lines in input file
        	// for example, if parsed int is 30000, it means we will do 30000 rounds 
        	// with each round a corresponding count
        	// the expect of count will be one estimate of e
        	long iterations = Integer.parseInt(value.toString());
        	int count = 0;
        	
        	for (int i = 0; i < iterations; i++){
        		double sum = 0.0;
        		while (sum < 1.0) {
        			sum += randomSeed.nextDouble();
        			count++;
        		}
        	}
        	
        	context.getCounter("Euler", "iterations").increment(iterations);
        	context.getCounter("Euler", "count").increment(count);
        	
        }
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "EulerEstimator");
        job.setJarByClass(EulerEstimator.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(EulerMapper.class);
        // no output file
        job.setOutputFormatClass(NullOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

