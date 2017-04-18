/* ******************************************************************************************
 * 
 * It will take not-one-record-per-line JSON input files:
 * 
 * {
 * "key1": "value1",
 * "key2": 2
 * }
 * {
 * "key1": "value3",
 * "key2": 4
 * }
 * 
 * and output single line JSON file:
 * 
 * {"key1": "value1", "key2": 2}
 * {"key1": "value3", "key2": 4}
 * 
 * so that RedditAverage.java can read in this TextInputFormat
 * 
 * 
 * ********************************************************************************************/
package tzu;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.common.base.Charsets;

public class MultiLineJSONInputFormat extends TextInputFormat {

	public class MultiLineRecordReader extends RecordReader<LongWritable, Text> {
		private LineRecordReader linereader;
		private LongWritable key;
		private Text value;

		public MultiLineRecordReader(byte[] recordDelimiterBytes) {
			linereader = new LineRecordReader(recordDelimiterBytes);
		}

		@Override
		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			linereader.initialize(genericSplit, context);
		}

		@Override		
		public boolean nextKeyValue() throws IOException {
			
			Text json = new Text("");

            while (linereader.nextKeyValue()) {
    			value = linereader.getCurrentValue();            	
            	json.append(value.copyBytes(), 0, value.getLength());
            	
            	if(value.toString().trim().equals("}")) {
            		value.set(json);
            		return true;
            	}
            
    			// if value start with "{" but also end with "}"
    			// then we think it's a one record in one line
    			if (value.find("{") == 0 &&
    					value.find("}") == value.getLength() - 1) {
            		return true;
    			}
            	
            }
            return false;
		}

		@Override
		public float getProgress() throws IOException {
			return linereader.getProgress();
		}

		@Override
		public LongWritable getCurrentKey() {
			return key;
		}

		@Override
		public Text getCurrentValue() {
			return value;
		}

		@Override
		public synchronized void close() throws IOException {
			linereader.close();
		}
	}

	// shouldn't have to change below here

	@Override
	public RecordReader<LongWritable, Text> 
	createRecordReader(InputSplit split,
			TaskAttemptContext context) {
		// same as TextInputFormat constructor, except return MultiLineRecordReader
		String delimiter = context.getConfiguration().get(
				"textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new MultiLineRecordReader(recordDelimiterBytes);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		// let's not worry about where to split within a file
		return false;
	}
}