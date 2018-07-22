package io.mymessaging.simple;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DefaultPullConsumer {
	private MessageStore messageStore;
	private MessageStoreConfig msc;
	private Set<String> buckets = new HashSet<String>();
	private ArrayList<String> bucketList = new ArrayList<String>();

	private HashMap<String, Long> queueOffsets = new HashMap<String, Long>();

	private HashMap<String, MappedFile> queueBuffers = new HashMap<String, MappedFile>();

	private String name;

	private int bucketIndex;

	public DefaultPullConsumer(String name, String storePath, Collection<String> topics, MessageStoreConfig msc) {
		try {
			this.setName(name);
			this.setMsc(msc);
			messageStore = MessageStore.getInstance(storePath);
			buckets.addAll(topics);
			bucketList.clear();
			bucketList.addAll(buckets);
			Collections.sort(bucketList);
			bucketIndex = 0;
			recover();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void recover() throws IOException {

		MappedFile.ensureDirOK(this.msc.getConsumerPosPathRootDir() + File.separatorChar + this.name);
		File dir = new File(this.msc.getConsumerPosPathRootDir() + File.separatorChar + this.name);
		File[] files = dir.listFiles();
		if (files != null) {
			for (File file : files) {

				try {
					MappedFile mappedFile = new MappedFile(file.getPath(), this.msc.getMapedFileSizeConsumerPos(),
							true);
					ByteBuffer bb = mappedFile.selectAllMappedBuffer(0);
					long size = bb.getLong();
					this.queueOffsets.put(file.getName(), size);
					this.queueBuffers.put(file.getName(), mappedFile);
				} catch (IOException e) {
					throw e;
				}
			}
		}

	}

	public ByteBuffer poll() throws IOException {
		return messageStore.getMessage(this);
	}

	public int getBucketIndex() {
		return bucketIndex;
	}

	public void setBucketIndex(int bucketIndex) {
		this.bucketIndex = bucketIndex;
	}

	public ArrayList<String> getBucketList() {
		return bucketList;
	}

	public void setBucketList(ArrayList<String> bucketList) {
		this.bucketList = bucketList;
	}

	public HashMap<String, Long> getQueueOffsets() {
		return queueOffsets;
	}

	public void setQueueOffsets(HashMap<String, Long> queueOffsets) {
		this.queueOffsets = queueOffsets;
	}

	public HashMap<String, MappedFile> getQueueBuffers() {
		return queueBuffers;
	}

	public void setQueueBuffers(HashMap<String, MappedFile> queueBuffers) {
		this.queueBuffers = queueBuffers;
	}

	public MessageStoreConfig getMsc() {
		return msc;
	}

	public void setMsc(MessageStoreConfig msc) {
		this.msc = msc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
