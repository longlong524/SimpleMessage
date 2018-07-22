package io.mymessaging.simple;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedFileQueue {

	private static final Logger log = LoggerFactory.getLogger("store");

	private final String storePath;

	private final int mappedFileSize;

	private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

	public AtomicInteger lastWiteIndex = new AtomicInteger();
	private boolean iscommitlog;

	public MappedFileQueue(final String storePath, int mappedFileSize, boolean iscommitlog) {
		this.storePath = storePath;
		this.mappedFileSize = mappedFileSize;
		this.iscommitlog = iscommitlog;
	}

	public boolean load() {
		File dir = new File(this.storePath);
		File[] files = dir.listFiles();
		List<MappedFile> tmp = new ArrayList<MappedFile>(1024);
		if (files != null) {
			// ascending order
			Arrays.sort(files);
			for (File file : files) {

				if (file.length() != this.mappedFileSize) {
					log.warn(file + "\t" + file.length() + " length not matched message store config value, ignore it");
					return true;
				}

				try {
					MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize, false);

					mappedFile.setWrotePosition(this.mappedFileSize);
					tmp.add(mappedFile);
					log.info("load " + file.getPath() + " OK");
				} catch (IOException e) {
					log.error("load file " + file + " error", e);
					return false;
				}
			}
			this.mappedFiles.addAll(tmp);
		}

		return true;
	}

	public long getMinOffset() {

		if (!this.mappedFiles.isEmpty()) {
			try {
				return this.mappedFiles.get(0).getFileFromOffset();
			} catch (IndexOutOfBoundsException e) {
			} catch (Exception e) {
			}
		}
		return -1;
	}

	public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
		try {
			MappedFile mappedFile = this.getFirstMappedFile();
			if (mappedFile != null) {
				int index = (int) ((offset / this.mappedFileSize)
						- (mappedFile.getFileFromOffset() / this.mappedFileSize));
				try {
					return this.mappedFiles.get(index);
				} catch (Exception e) {
					if (returnFirstOnNotFound) {
						return mappedFile;
					}
				}
			}
		} catch (Exception e) {
		}
		return null;
	}

	public MappedFile getFirstMappedFile() {
		MappedFile mappedFileFirst = null;

		if (!this.mappedFiles.isEmpty()) {
			try {
				mappedFileFirst = this.mappedFiles.get(0);
			} catch (IndexOutOfBoundsException e) {
			} catch (Exception e) {
			}
		}
		return mappedFileFirst;
	}

	public MappedFile findMappedFileByOffset(final long offset) {
		return findMappedFileByOffset(offset, false);
	}

	public void writeMsg(long offset, int size) {
		while (true) {
			int last = lastWiteIndex.get();
			MappedFile mm = null;
			if (last < mappedFiles.size()) {
				mm = mappedFiles.get(last);
			}
			int off = 0;
			if (mm == null || (off = mm.appendMessage(offset, size)) == -1) {
				try {
					if (off == -1) {
						this.lastWiteIndex.compareAndSet(last, last + 1);
					}
					if (!this.iscommitlog) {
						// 防止两个线程同时创建文件
						synchronized (this) {
							MappedFile new_last_mm = mappedFiles.size() == 0 ? null
									: mappedFiles.get(mappedFiles.size() - 1);
							if (new_last_mm == mm) {
								if (mm == null) {
									mm = new MappedFile(this.storePath + File.separatorChar + "00000000000000000000",
											this.mappedFileSize, false);
								} else {
									mm = new MappedFile(
											this.storePath + File.separatorChar
													+ String.format("%020d",
															mm.getFileFromOffset() + this.mappedFileSize),
											this.mappedFileSize, false);
								}
								mm.warmWriteMappedFile();

								this.mappedFiles.add(mm);
							}
						}
					}
					continue;
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else {
				return;
			}
		}
	}

	public long writeMsg(ByteBuffer bb) {
		while (true) {
			int last = lastWiteIndex.get();
			MappedFile mm = null;
			if (last < mappedFiles.size()) {
				mm = mappedFiles.get(last);
			}
			long offset = 0;

			if (mm == null || (offset = mm.appendMessage(bb)) == -1) {
				try {
					if (offset == -1) {
						this.lastWiteIndex.compareAndSet(last, last + 1);
					}
					if (!this.iscommitlog) {
						// 防止两个线程同时创建文件
						synchronized (this) {
							MappedFile new_last_mm = mappedFiles.size() == 0 ? null
									: mappedFiles.get(mappedFiles.size() - 1);
							if (new_last_mm == mm) {
								if (mm == null) {
									mm = new MappedFile(this.storePath + File.separatorChar + "00000000000000000000",
											this.mappedFileSize, false);
								} else {
									mm = new MappedFile(
											this.storePath + File.separatorChar
													+ String.format("%020d",
															mm.getFileFromOffset() + this.mappedFileSize),
											this.mappedFileSize, false);
								}
								mm.warmWriteMappedFile();

								this.mappedFiles.add(mm);

							}
						}
					}
					continue;
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else {
				return offset;
			}
		}
	}

	public List<MappedFile> getMappedFiles() {
		return mappedFiles;
	}

	public int getMappedFileSize() {
		return mappedFileSize;
	}

	public String getStorePath() {
		return storePath;
	}

}
