package io.mymessaging.simple;

import java.nio.ByteBuffer;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog {

	private final MappedFileQueue mappedFileQueue;

	private MessageStoreConfig messageStoreConfig;

	private String storePathCommitLog;

	private int mapFileSizeCommitLog;

	public CommitLog(MessageStoreConfig messageStoreConfig2) {
		this.storePathCommitLog = messageStoreConfig2.getStorePathCommitLog();
		this.mapFileSizeCommitLog = messageStoreConfig2.getMapedFileSizeCommitLog();
		this.messageStoreConfig = messageStoreConfig2;
		this.mappedFileQueue = new MappedFileQueue(storePathCommitLog, mapFileSizeCommitLog, true);
	}

	boolean load() {
		boolean result = this.mappedFileQueue.load();
		return result;
	}

	public void recover(long pos, int size) {
		if (pos == 0 && size == 0) {
			return;
		}
		int mappedFileSize = this.getMessageStoreConfig().getMapedFileSizeCommitLog();
		int lastp = (int) (pos + size - 1) / mappedFileSize;
		pos = (pos + size) % mappedFileSize;
		this.mappedFileQueue.lastWiteIndex.set(lastp);
		this.mappedFileQueue.getMappedFiles().get(lastp).wrotePosition.set((int) pos);
	}

	public void shutdown() {

	}

	public ByteBuffer getData(final long offset) {
		MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
		if (mappedFile != null) {
			int pos = (int) (offset % this.mapFileSizeCommitLog);
			return mappedFile.selectMappedBuffer(pos);
		}
		return null;
	}

	public long putMessage(ByteBuffer bb) {
		return mappedFileQueue.writeMsg(bb);
	}

	public long rollNextFile(final long offset) {
		int mappedFileSize = this.getMessageStoreConfig().getMapedFileSizeCommitLog();
		return offset + mappedFileSize - offset % mappedFileSize;
	}

	public long getMinOffset() {
		MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
		if (mappedFile != null) {
			return mappedFile.getFileFromOffset();
		}
		return -1;
	}

	public boolean flush(int flushConsumeQueueLeastPages) {
		return true;
	}

	public MessageStoreConfig getMessageStoreConfig() {
		return messageStoreConfig;
	}

	public void setMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
		this.messageStoreConfig = messageStoreConfig;
	}

	public String getStorePathCommitLog() {
		return storePathCommitLog;
	}

	public void setStorePathCommitLog(String storePathCommitLog) {
		this.storePathCommitLog = storePathCommitLog;
	}

	public int getMapFileSizeCommitLog() {
		return mapFileSizeCommitLog;
	}

	public void setMapFileSizeCommitLog(int mapFileSizeCommitLog) {
		this.mapFileSizeCommitLog = mapFileSizeCommitLog;
	}

	public MappedFileQueue getMappedFileQueue() {
		return mappedFileQueue;
	}

}
