package io.mymessaging.simple;

import java.nio.ByteBuffer;



public class TopicIndexLog {
	public static final int CQ_STORE_UNIT_SIZE = 12;


	private final MappedFileQueue mappedFileQueue;

	private MessageStoreConfig messageStoreConfig;

	private String storePathIndexLog;
	private int mapFileSizeIndexLog;

	public TopicIndexLog(MessageStoreConfig messageStoreConfig2, String topic) {
		this.storePathIndexLog = messageStoreConfig2.getIndexPathRootDir();
		this.mapFileSizeIndexLog = messageStoreConfig2.getMapedFileSizeConsumeQueue();
		this.messageStoreConfig = messageStoreConfig2;
		this.mappedFileQueue = new MappedFileQueue(storePathIndexLog + "/" + topic, mapFileSizeIndexLog, false);

	}

	boolean load() {
		boolean result = this.mappedFileQueue.load();
		return result;
	}

	public String recover() {
		String recover = "0:0";
		if (this.mappedFileQueue.getMappedFiles().size() == 0) {
			return recover;
		}
		for (int ss = this.mappedFileQueue.getMappedFiles().size() - 2; ss < this.mappedFileQueue.getMappedFiles()
				.size(); ss++) {
			if (ss < 0) {
				continue;
			}
			MappedFile mf = this.mappedFileQueue.getMappedFiles().get(ss);
			ByteBuffer bb = mf.selectMappedBuffer(0);
			bb.limit(mf.getFileSize());
			for (; bb.hasRemaining();) {
				long pos = bb.getLong();
				int size = bb.getInt();
				if (size == 0) {
					this.mappedFileQueue.lastWiteIndex.set(ss);
					mf.wrotePosition.set(bb.position() - 12);
					return recover;

				}
				recover = pos + ":" + size;
			}
		}
		this.mappedFileQueue.lastWiteIndex.set(this.mappedFileQueue.getMappedFiles().size() - 1);
		MappedFile mf = this.mappedFileQueue.getMappedFiles().get(this.mappedFileQueue.getMappedFiles().size() - 1);
		mf.wrotePosition.set(mf.fileSize);
		return recover;
	}

	public ByteBuffer getData(final long offset) {
		MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
		if (mappedFile != null) {
			int pos = (int) (offset % this.mapFileSizeIndexLog);
			return mappedFile.selectMappedBuffer(pos);
		}

		return null;
	}

	public void putMessage(long offset, int size) {
		mappedFileQueue.writeMsg(offset, size);
	}

	public long rollNextFile(final long offset) {
		int mappedFileSize = this.getMessageStoreConfig().getMapedFileSizeConsumeQueue();
		return offset + mappedFileSize - offset % mappedFileSize;
	}

	public MessageStoreConfig getMessageStoreConfig() {
		return messageStoreConfig;
	}

	public void setMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
		this.messageStoreConfig = messageStoreConfig;
	}

	public long putMessage(ByteBuffer bb) {
		return mappedFileQueue.writeMsg(bb);
	}

}
