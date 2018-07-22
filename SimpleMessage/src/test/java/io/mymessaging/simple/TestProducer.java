package io.mymessaging.simple;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import io.mymessaging.simple.DefaultPullConsumer;
import io.mymessaging.simple.MessageStore;
import io.mymessaging.simple.MessageStoreConfig;

public class TestProducer {

	@Test
	public void testProducer() throws Exception {
		MessageStoreConfig cc = new MessageStoreConfig();
		MessageStore ms = MessageStore.getInstance(cc.getStorePathRootDir());
		
		
		for (int i = 0; i < 100; i++) {
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.clear();
			bb.put(("hello" + i).getBytes());
			bb.flip();
			ms.putMessage("topic1", bb);
		}

		ms.shutdown();
	}

	@Test
	public void testConsume() throws Exception {
		MessageStoreConfig cc = new MessageStoreConfig();
		MessageStore ms = MessageStore.getInstance(cc.getStorePathRootDir());
		List<String> topics = new LinkedList<String>();
		topics.add("topic1");
		DefaultPullConsumer dpc = new DefaultPullConsumer("consumer1", cc.getStorePathRootDir(), topics, cc);

		for (int i = 0;; i++) {
			ByteBuffer bb = dpc.poll();
			if (bb == null) {
				break;
			}
			byte[] bytes = new byte[bb.remaining()];
			bb.get(bytes);
			System.err.println(new String(bytes, "utf-8"));
		}
		ms.shutdown();
	}
}
