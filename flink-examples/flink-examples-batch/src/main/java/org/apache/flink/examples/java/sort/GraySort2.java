/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.sort.util.GraySortHadoopInputFormat;
import org.apache.flink.examples.java.sort.util.GraySortHadoopOutputFormat;
import org.apache.flink.examples.java.sort.util.RecordKey;
import org.apache.flink.examples.java.sort.util.RecordValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class GraySort2 {
	public static void main(String[] args) throws Exception {

		// Get input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.setGlobalJobParameters(params);                     // Make parameters available in the web interface
		executionConfig.setRestartStrategy(RestartStrategies.noRestart());  // Disable failed task retry
		// executionConfig.setExecutionMode(ExecutionMode.BATCH);              // Make shuffles in batch fashion, only local data exchange will be in pipeline mode
		executionConfig.setExecutionMode(ExecutionMode.PIPELINED);
		executionConfig.enableObjectReuse();                                // Enable object reuse for better memory performance
		// Use Kryo to serialize, But the more efficient way would be make RecordKey and RecordValue implement Value type
		//executionConfig.enableForceKryo();
		//executionConfig.registerKryoType(RecordKey.class);
		//executionConfig.registerKryoType(RecordValue.class);

		// User pre defined String
		final String hdfs = params.get("hdfs", "hdfs://10.1.2.67:8020");
		final String inputPathStr = params.get("input", "/home/maowei/TestData/input");
		final String outputPathStr = params.get("output", "/home/maowei/TestData/resultRecord2/");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		Job job = Job.getInstance(conf);
		GraySortHadoopOutputFormat.setOutputPath(job, new Path(hdfs + outputPathStr));
		job.setOutputKeyClass(RecordKey.class);
		job.setOutputValueClass(RecordValue.class);
		job.setInputFormatClass(GraySortHadoopInputFormat.class);
		job.setOutputFormatClass(GraySortHadoopOutputFormat.class);

		DataSource<Tuple2<RecordKey, RecordValue>> source = env.readHadoopFile(new GraySortHadoopInputFormat(), RecordKey.class, RecordValue.class, inputPathStr, job);
		DataSet<Tuple2<RecordKey, RecordValue>> sortedData = source.partitionByRange(0).sortPartition(0, Order.ASCENDING);

		sortedData.output(new HadoopOutputFormat<RecordKey, RecordValue>(new GraySortHadoopOutputFormat(), job));
		env.execute();
		//System.out.println("record number: " + sortedData.count());
	}

}
