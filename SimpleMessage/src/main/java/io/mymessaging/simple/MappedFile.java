package io.mymessaging.simple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MappedFile {

	public static final int OS_PAGE_SIZE = 1024 * 4;

	public final AtomicInteger wrotePosition = new AtomicInteger(0);
	protected int fileSize = OS_PAGE_SIZE * 32;
	protected FileChannel fileChannel;

	private AtomicBoolean preread = new AtomicBoolean(false);

	/**
	 * Message will put to here first, and then reput to FileChannel if writeBuffer
	 * is not null.
	 */
	private String fileName;
	private long fileFromOffset;
	private File file;
	private MappedByteBuffer mappedByteBuffer;

	public MappedFile() {

	}

	public MappedFile(final String fileName, final int filesize, boolean posFile) throws IOException {
		init(fileName, filesize, posFile);
	}

	public static void ensureDirOK(final String dirName) {
		if (dirName != null) {
			File f = new File(dirName);
			if (!f.exists()) {
				f.mkdirs();
			}
		}
	}

	/**
	 * clean this direct buffer
	 * 
	 * @param buffer
	 */
	public static void clean(final ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
			return;
		invoke(invoke(viewed(buffer), "cleaner"), "clean");
	}

	private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
		return AccessController.doPrivileged(new PrivilegedAction<Object>() {
			public Object run() {
				try {
					Method method = method(target, methodName, args);
					method.setAccessible(true);
					return method.invoke(target);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
	}

	private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
		try {
			return target.getClass().getMethod(methodName, args);
		} catch (NoSuchMethodException e) {
			return target.getClass().getDeclaredMethod(methodName, args);
		}
	}

	private static ByteBuffer viewed(ByteBuffer buffer) {
		String methodName = "viewedBuffer";

		Method[] methods = buffer.getClass().getMethods();
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals("attachment")) {
				methodName = "attachment";
				break;
			}
		}

		ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
		if (viewedBuffer == null)
			return buffer;
		else
			return viewed(viewedBuffer);
	}

	void init(final String fileName, final int filesize, boolean posFile) throws IOException {
		this.fileName = fileName;
		this.fileSize = filesize;
		this.file = new File(fileName);
		if (!posFile) {
			this.fileFromOffset = Long.parseLong(this.file.getName());
		}

		boolean ok = false;

		ensureDirOK(this.file.getParent());

		try {
			this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
			this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
			this.fileChannel.position(0);
			ok = true;
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			if (!ok && this.fileChannel != null) {
				this.fileChannel.close();
			}
		}
	}

	/**
	 * append a long and int
	 * 
	 * @param offset
	 * @param size
	 * @return
	 */
	public int appendMessage(long offset, int size) {
		if (this.isFull()) {
			return -1;
		}
		int msgLen = 12;
		int currentPos = this.wrotePosition.getAndAdd(msgLen);
		if (currentPos + msgLen > this.fileSize) {
			return -1;
		}

		ByteBuffer byteBuffer = mappedByteBuffer.duplicate();
		byteBuffer.position(currentPos);
		byteBuffer.putLong(offset);
		byteBuffer.putInt(size);

		return 1;
	}

	public long appendMessage(ByteBuffer bb) {
		if (this.isFull()) {
			return -1;
		}
		int msgLen = bb.limit();
		int currentPos = this.wrotePosition.getAndAdd(msgLen);
		if (currentPos + msgLen > this.fileSize) {
			return -1;
		}

		bb.position(0);
		ByteBuffer byteBuffer = mappedByteBuffer.duplicate();
		byteBuffer.position(currentPos);
		byteBuffer.put(bb);
		return currentPos + this.fileFromOffset;
	}

	public ByteBuffer selectMappedBuffer(int pos) {
		if (this.preread.compareAndSet(false, true)) {
			this.warmReadMappedFile();
		}
		int readPosition = getReadPosition();
		if (pos < readPosition && pos >= 0) {
			ByteBuffer byteBuffer = this.mappedByteBuffer.duplicate();
			byteBuffer.position(pos);
			byteBuffer.limit(readPosition);
			return byteBuffer;
		}

		return null;
	}

	public ByteBuffer selectAllMappedBuffer(int pos) {
		if (this.preread.compareAndSet(false, true)) {
			this.warmReadMappedFile();
		}
		{
			ByteBuffer byteBuffer = this.mappedByteBuffer.duplicate();
			byteBuffer.clear();
			byteBuffer.position(pos);
			return byteBuffer;
		}

	}

	/**
	 * 预热到内存中
	 */
	public void warmWriteMappedFile() {
		ByteBuffer byteBuffer = this.mappedByteBuffer.duplicate();
		for (int i = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE) {
			byteBuffer.put(i, (byte) 0);
		}
	}

	/**
	 * 预热mappedfile到内存中
	 */
	public void warmReadMappedFile() {
		ByteBuffer byteBuffer = this.mappedByteBuffer.duplicate();
		for (int i = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE) {
			byteBuffer.get(i);
		}
	}

	public int getFileSize() {
		return fileSize;
	}

	public FileChannel getFileChannel() {
		return fileChannel;
	}

	public long getFileFromOffset() {
		return this.fileFromOffset;
	}

	/**
	 * max position which has valid value
	 * 
	 * @return
	 */
	public int getReadPosition() {
		return this.wrotePosition.get();
	}

	public boolean isFull() {
		return this.fileSize <= this.wrotePosition.get();
	}

	public boolean cleanup() {
		clean(this.mappedByteBuffer);
		return true;
	}

	public int getWrotePosition() {
		return wrotePosition.get();
	}

	public void setWrotePosition(int pos) {
		this.wrotePosition.set(pos);
	}

	public String getFileName() {
		return fileName;
	}

	@Override
	public String toString() {
		return this.fileName;
	}

}
