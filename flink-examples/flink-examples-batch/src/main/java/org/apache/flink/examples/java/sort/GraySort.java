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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class GraySort {
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		String hdfs = params.get("hdfs", "hdfs://10.1.2.67:8020");
		String inputPathStr = params.get("input", "/home/maowei/TestData/input");
		String outputPathStr = params.get("output", "/home/maowei/TestData/resultText/");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		// conf.addResource("/home/maowei/mirror/hadoop/hadoop-2.7.2/etc/hadoop/core-site.xml");
		Job job = Job.getInstance(conf);
		TeraInputFormat.setInputPaths(job, hdfs + inputPathStr);
		TeraOutputFormat.setOutputPath(job, new Path(hdfs + outputPathStr));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TeraInputFormat.class);
		job.setOutputFormatClass(TeraOutputFormat.class);

		DataSource<Tuple2<Text, Text>> source = env.readHadoopFile(new TeraInputFormat(), Text.class, Text.class, inputPathStr, job).setParallelism(4);
		DataSet<Tuple2<Text, Text>> sortedData = source.partitionByRange(0).setParallelism(4).sortPartition(0, Order.ASCENDING).setParallelism(14);

		sortedData.output(new HadoopOutputFormat<Text, Text>(new TeraOutputFormat(), job)).setParallelism(14);
		env.execute();
	}

}
