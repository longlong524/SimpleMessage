/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mymessaging.simple;

import java.io.File;

public class MessageStoreConfig {
	// The root directory in which the log data is kept
	private String storePathRootDir = System.getProperty("user.home") + File.separator + "mymessage" + File.separator
			+ "store";

	// The directory in which the commitlog is kept
	private String storePathCommitLog = storePathRootDir + File.separator + "commitlog";

	// index path
	private String indexPathRootDir = storePathRootDir + File.separator + "index";

	// CommitLog file size,default is 512K
	private int mapedFileSizeCommitLog = 1024 * 512;

	// ConsumeQueue file size,default is 30W
	private int mapedFileSizeConsumeQueue = 10 * MappedFile.OS_PAGE_SIZE * TopicIndexLog.CQ_STORE_UNIT_SIZE;

	//
	private int mapedFileSizeConsumerPos = 1024;

	// index path
	private String consumerPosPathRootDir = storePathRootDir + File.separator + "consumer";

	public int getMapedFileSizeCommitLog() {
		return mapedFileSizeCommitLog;
	}

	public void setMapedFileSizeCommitLog(int mapedFileSizeCommitLog) {
		this.mapedFileSizeCommitLog = mapedFileSizeCommitLog;
	}

	public String getIndexPathRootDir() {
		return indexPathRootDir;
	}

	public void setIndexPathRootDir(String indexPathRootDir) {
		this.indexPathRootDir = indexPathRootDir;
	}

	public String getStorePathCommitLog() {
		return storePathCommitLog;
	}

	public void setStorePathCommitLog(String storePathCommitLog) {
		this.storePathCommitLog = storePathCommitLog;
	}

	public String getStorePathRootDir() {
		return storePathRootDir;
	}

	public void setStorePathRootDir(String storePathRootDir) {
		this.storePathRootDir = storePathRootDir;
	}

	public int getMapedFileSizeConsumeQueue() {
		return mapedFileSizeConsumeQueue;
	}

	public void setMapedFileSizeConsumeQueue(int mapedFileSizeConsumeQueue) {
		this.mapedFileSizeConsumeQueue = mapedFileSizeConsumeQueue;
	}

	public int getMapedFileSizeConsumerPos() {
		return mapedFileSizeConsumerPos;
	}

	public void setMapedFileSizeConsumerPos(int mapedFileSizeConsumerPos) {
		this.mapedFileSizeConsumerPos = mapedFileSizeConsumerPos;
	}

	public String getConsumerPosPathRootDir() {
		return consumerPosPathRootDir;
	}

	public void setConsumerPosPathRootDir(String consumerPosPathRootDir) {
		this.consumerPosPathRootDir = consumerPosPathRootDir;
	}

}
