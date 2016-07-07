package org.apache.flink.examples.java.sort.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.List;

/**
 * Created by maowei on 7/4/16.
 */
public class GraySortHadoopInputFormat extends FileInputFormat<RecordKey, RecordValue> {
	static final String PARTITION_FILENAME = "_partition.lst";
	static final int KEY_LENGTH = RecordKey.KEY_SIZE;
	static final int VALUE_LENGTH = RecordValue.VALUE_SIZE;
	static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;

	private static MRJobConfig lastContext = null;
	private static List<InputSplit> lastResult = null;

	// implement abstract method
	@Override
	public RecordReader<RecordKey, RecordValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new GraySortReader();
	}

	// defined an Reader
	static class GraySortReader extends RecordReader<RecordKey, RecordValue> {
		private FSDataInputStream in;
		private long offset;
		private long length;
		private RecordKey key;
		private RecordValue value;

		public GraySortReader() throws IOException{
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			Path p = ((FileSplit)split).getPath();
			FileSystem fs = p.getFileSystem(context.getConfiguration());
			in = fs.open(p);
			offset = 0;
			length = ((FileSplit)split).getLength();
			key = new RecordKey();
			value = new RecordValue();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(offset >= length) {
				return false;
			}
			in.read(key.key, 0, RecordKey.KEY_SIZE );
			in.read(value.value, 0, RecordValue.VALUE_SIZE);
			offset += RECORD_LENGTH;
			return true;
		}

		@Override
		public RecordKey getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public RecordValue getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) offset / length;
		}

		@Override
		public void close() throws IOException {
			in.close();
		}
	}
}
