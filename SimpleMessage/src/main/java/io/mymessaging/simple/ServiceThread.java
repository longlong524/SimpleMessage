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

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ServiceThread implements Runnable {
	private static final long JOIN_TIME = 90 * 1000;

	protected final Thread thread;
	protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
	protected volatile boolean stopped = false;

	public ServiceThread() {
		this.thread = new Thread(this, this.getServiceName());
	}

	public abstract String getServiceName();

	public void start() {
		this.thread.start();
	}

	public void shutdown() {
		this.shutdown(false);
	}

	public void shutdown(final boolean interrupt) {
		this.stopped = true;

		try {
			if (interrupt) {
				this.thread.interrupt();
			}

			if (!this.thread.isDaemon()) {
				this.thread.join(this.getJointime());
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public long getJointime() {
		return JOIN_TIME;
	}

	public void stop() {
		this.stop(false);
	}

	public void stop(final boolean interrupt) {
		this.stopped = true;

		if (interrupt) {
			this.thread.interrupt();
		}
	}

	public void makeStop() {
		this.stopped = true;
	}

	public void wakeup() {

	}

	protected void waitForRunning(long interval) {
		if (hasNotified.compareAndSet(true, false)) {
			this.onWaitEnd();
			return;
		}

	}

	protected void onWaitEnd() {
	}

	public boolean isStopped() {
		return stopped;
	}
}
