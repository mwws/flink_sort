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
import org.apache.flink.types.Key;

import java.io.IOException;

/**
 * Created by maowei on 6/30/16.
 */
public final class RecordKey implements Key<RecordKey> {
	/**
	 * The size of key in bytes
	 */
	public static final int KEY_SIZE = 10;

	/**
	 * The buffer to store the key
 	 */
	public byte[] key;

	/**
	 * Default constructor required for serialization and deserialization
	 */
	public RecordKey() {
		this.key = new byte[KEY_SIZE];
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.write(this.key, 0, KEY_SIZE);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		in.readFully(this.key, 0, KEY_SIZE);
	}

	@Override
	public int compareTo(RecordKey o) {
		int diff = 0;
		for (int i = 0; i < KEY_SIZE; i++) {
			diff = this.key[i] - o.key[i];
			if (diff != 0) {
				break;
			}
		}
		return diff;
	}

	@Override
	public boolean equals(Object obj) {
		if (getClass() != obj.getClass()) {
			return false;
		}

		final RecordKey other = (RecordKey) obj;
		for (int i = 0; i < KEY_SIZE; i++) {
			if (this.key[i] != other.key[i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		return new String(this.key, 0, KEY_SIZE);
	}

}
