# SimpleMessage
一个简单的磁盘消息存储引擎

# 用途
程序中的持久化存储解决方案


# 使用

## put 消息

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
    
## get消息

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
  
  # 架构
  
  把阿里巴巴的rocketmq的store部分进行精简，只保留磁盘存储部分，
