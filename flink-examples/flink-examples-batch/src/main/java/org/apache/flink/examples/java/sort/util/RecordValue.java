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

package org.apache.flink.examples.java.sort.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

/**
 * Created by maowei on 6/30/16.
 */
public final class RecordValue implements Value{
	/**
	 * The size of value in bytes
	 */
	public static final int VALUE_SIZE = 90;

	/**
	 * The buffer to store the value
	 */
	public byte[] value;

	/**
	 * Default constructor required for serialization and deserialization
	 */
	public RecordValue() {
		this.value = new byte[VALUE_SIZE];
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(this.value, 0, VALUE_SIZE);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		in.readFully(this.value, 0, VALUE_SIZE);
	}

	@Override
	public String toString() {
		return new String(this.value, 0, VALUE_SIZE);
	}
}
