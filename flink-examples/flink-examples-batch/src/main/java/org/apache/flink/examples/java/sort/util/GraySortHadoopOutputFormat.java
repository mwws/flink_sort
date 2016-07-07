package org.apache.flink.examples.java.sort.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;

public class GraySortHadoopOutputFormat extends FileOutputFormat<RecordKey, RecordValue> {

	@Override
	public RecordWriter<RecordKey, RecordValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Path file = getDefaultWorkFile(context, "");
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		FSDataOutputStream out = fs.create(file);
		return new GraySortWriter(out, context);
	}

	@Override
	public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
		// Ensure that the output directory is set
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set in JobConf.");
		}

		final Configuration jobConf = job.getConfiguration();

		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(),
			new Path[] { outDir }, jobConf);

		final FileSystem fs = outDir.getFileSystem(jobConf);

		if (fs.exists(outDir)) {
			// existing output dir is considered empty iff its only content is the
			// partition file.
			final FileStatus[] outDirKids = fs.listStatus(outDir);
			boolean empty = false;
			if (outDirKids != null && outDirKids.length == 1) {
				final FileStatus st = outDirKids[0];
				final String fname = st.getPath().getName();
				empty =
					!st.isDirectory() && GraySortHadoopInputFormat.PARTITION_FILENAME.equals(fname);
			}
			if (!empty) {
				throw new FileAlreadyExistsException("Output directory " + outDir
					+ " already exists");
			}
		}
	}

	//define a Writer
	static class GraySortWriter extends RecordWriter<RecordKey, RecordValue> {
		private FSDataOutputStream out;

		public GraySortWriter(FSDataOutputStream out,
								JobContext job) {
			this.out = out;
		}

		public synchronized void write(RecordKey key, RecordValue value) throws IOException {
			out.write(key.key, 0, RecordKey.KEY_SIZE);
			out.write(value.value, 0, RecordValue.VALUE_SIZE);
		}

		public void close(TaskAttemptContext context) throws IOException {
			out.close();
		}
	}
}
