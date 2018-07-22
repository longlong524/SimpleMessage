package io.mymessaging.simple;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStore {

	private static final Logger log = LoggerFactory.getLogger("store");

	private MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

	private final Map<String, TopicIndexLog> indexLogs = new ConcurrentHashMap<String, TopicIndexLog>();

	private CommitLog test_com;

	private CreateFileService createCommitLogService = new CreateFileService();

	// 保存consume订阅的bucket列表
	private static volatile MessageStore INSTANCE;
	private boolean shutdown;

	public static synchronized MessageStore getInstance(String path) throws Exception {
		if (INSTANCE == null) {
			MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
			messageStoreConfig.setStorePathRootDir(path);
			messageStoreConfig.setStorePathCommitLog(path + File.separator + "commitlog");
			messageStoreConfig.setIndexPathRootDir(path + File.separator + "index");

			INSTANCE = new MessageStore(messageStoreConfig);
			INSTANCE.load();
			INSTANCE.start();
		}
		return INSTANCE;
	}

	private MessageStore(final MessageStoreConfig messageStoreConfig) throws IOException {

		this.messageStoreConfig = messageStoreConfig;
		String comm = messageStoreConfig.getIndexPathRootDir();
		File ff = new File(comm);
		ff.mkdirs();
		String[] fs = ff.list();
		if (fs != null) {
			for (String commit_file : fs) {
				indexLogs.putIfAbsent(commit_file, new TopicIndexLog(messageStoreConfig, commit_file));
			}
		}
		test_com = new CommitLog(messageStoreConfig);
	}

	/**
	 * @throws IOException
	 */
	private boolean load() {
		try {
			for (TopicIndexLog commitLog : indexLogs.values()) {
				commitLog.load();
			}
			log.info("load index finished " + this.messageStoreConfig.getIndexPathRootDir());
			test_com.load();
			log.info("load commitlog finished " + this.messageStoreConfig.getStorePathCommitLog());
			recover();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private void recover() {
		long pos = 0;
		int size = 0;
		for (TopicIndexLog indexLog : indexLogs.values()) {
			String recover = indexLog.recover();
			String[] args = recover.split(":");
			log.info("recover index " + indexLog + " finished " + recover);

			Long ll = Long.parseLong(args[0]);
			if (ll > pos) {
				pos = ll;
				size = Integer.parseInt(args[1]);
			}
		}
		this.test_com.recover(pos, size);
		log.info("recover commitlog " + " finished " + pos + ":" + size);

	}

	private void start() throws Exception {

		createCommitLogService.start();
		this.shutdown = false;
	}

	public void shutdown() {
		if (!this.shutdown) {
			this.shutdown = true;
			createCommitLogService.shutdown();
		}
	}

	public void putMessage(String bucket, ByteBuffer message) {

		if (indexLogs.get(bucket) == null) {
			synchronized (bucket) {
				if (indexLogs.get(bucket) == null) {
					indexLogs.put(bucket, new TopicIndexLog(messageStoreConfig, bucket));
				}
			}
		}
		int size = message.limit();
		long offset = test_com.putMessage(message);
		writeIndexLog(bucket, offset, size);

	}

	private void writeIndexLog(String bucket, long offset, int size) {
		indexLogs.get(bucket).putMessage(offset, size);
	}

	public ByteBuffer getMessage(final DefaultPullConsumer consumer) throws IOException {
		// 订阅列表
		ArrayList<String> bucketList = consumer.getBucketList();
		int loop = 0;
		int index = consumer.getBucketIndex();
		HashMap<String, Long> queueOffsets = consumer.getQueueOffsets();
		Map<String, MappedFile> mfs = consumer.getQueueBuffers();
		while (true) {
			index = index % bucketList.size();
			if (loop++ >= bucketList.size()) {
				return null;
			}

			String bucket = bucketList.get(index);
			TopicIndexLog cl = this.indexLogs.get(bucket);
			if (cl == null) {
				index++;
				continue;
			}
			if (queueOffsets.get(bucket) == null) {
				queueOffsets.put(bucket, (long) 0);
				mfs.put(bucket,
						new MappedFile(
								consumer.getMsc().getConsumerPosPathRootDir() + File.separatorChar + consumer.getName()
										+ File.separatorChar + bucket,
								consumer.getMsc().getMapedFileSizeConsumerPos(), true));
			}
			long consumer_index = queueOffsets.get(bucket);
			ByteBuffer bb = cl.getData(consumer_index * TopicIndexLog.CQ_STORE_UNIT_SIZE);
			if (bb == null) {
				index++;
				continue;
			}

			long dataoffset = bb.getLong();
			int sizePy = bb.getInt();
			if (sizePy == 0) {
				index++;
				continue;
			}
			ByteBuffer tmp_bb = this.test_com.getData(dataoffset);

			{
				try {
					tmp_bb.limit(tmp_bb.position() + sizePy);
					queueOffsets.put(bucket, consumer_index + 1);
					consumer.setBucketIndex((index + 1) % bucketList.size());
					mfs.get(bucket).selectAllMappedBuffer(0).putLong(consumer_index + 1);
					return tmp_bb;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			return null;

		}
	}

	class CreateFileService extends ServiceThread {
		public void run() {
			while (true) {
				if (super.stopped) {
					return;
				}
				List<MappedFile> ll = MessageStore.this.test_com.getMappedFileQueue().getMappedFiles();
				String storepath = MessageStore.this.test_com.getMappedFileQueue().getStorePath();
				int mappedfilesize = MessageStore.this.test_com.getMappedFileQueue().getMappedFileSize();
				AtomicInteger lastIndex = MessageStore.this.test_com.getMappedFileQueue().lastWiteIndex;
				if (lastIndex.get() >= ll.size() - 5) {
					for (int i = 0; i < 10; i++) {
						MappedFile last_mm = ll.size() == 0 ? null : ll.get(ll.size() - 1);
						MappedFile mm = null;
						try {
							String name = String.format("%020d",
									(last_mm == null ? 0 : (last_mm.getFileFromOffset() + mappedfilesize)));
							mm = new MappedFile(storepath + File.separatorChar + name, mappedfilesize, true);
						} catch (IOException e) {
							e.printStackTrace();
						}

						mm.warmWriteMappedFile();
						ll.add(mm);

					}

				}

				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public String getServiceName() {
			return "createFileService";
		}

	}

	public MessageStoreConfig getMessageStoreConfig() {
		return messageStoreConfig;
	}

	public void setMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
		this.messageStoreConfig = messageStoreConfig;
	}
}
