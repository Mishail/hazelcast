/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.util.executor;

import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @mdogan 12/17/12
 */
public class FastExecutorImpl2 implements FastExecutor {

    //    private final BlockingQueue<WorkerTask> queue;
    private final Queue<WorkerTask> queue;
    private final Collection<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
    private final int queueCapacity;
    private final ThreadFactory threadFactory;
    private final int coreThreadSize;
    private final int maxThreadSize;
    private final long backlogInterval;
    private final long keepAliveMillis;
    private final boolean allowCoreThreadTimeout;
    private final Lock lock = new ReentrantLock();
    private final Condition signalWorker = lock.newCondition();

    private volatile WorkerLifecycleInterceptor interceptor;
    private volatile int activeThreadCount;
    private volatile boolean live = true;

    public FastExecutorImpl2(int coreThreadSize, String namePrefix, ThreadFactory threadFactory) {
        this(coreThreadSize, coreThreadSize * 20, Math.max(Integer.MAX_VALUE, coreThreadSize * (1 << 16)),
                500L, namePrefix, threadFactory, TimeUnit.SECONDS.toMillis(60), false, true);
    }

    public FastExecutorImpl2(int coreThreadSize, int maxThreadSize, int queueCapacity,
                             long backlogIntervalInMillis, String namePrefix, ThreadFactory threadFactory,
                             long keepAliveMillis, boolean allowCoreThreadTimeout, boolean startCoreThreads) {
        this.queueCapacity = queueCapacity;
        this.threadFactory = threadFactory;
        this.keepAliveMillis = keepAliveMillis;
        this.coreThreadSize = coreThreadSize;
        this.maxThreadSize = maxThreadSize;
        this.backlogInterval = backlogIntervalInMillis;
        this.allowCoreThreadTimeout = allowCoreThreadTimeout;
        this.queue = new LinkedBlockingQueue<WorkerTask>(queueCapacity);
//        this.queue = new ConcurrentLinkedQueue<WorkerTask>();

        Thread t = new Thread(new BacklogDetector(), namePrefix + "backlog");
        threads.add(t);
        if (startCoreThreads) {
            t.start();
        }
        for (int i = 0; i < coreThreadSize; i++) {
            addWorker(startCoreThreads);
        }
    }

    public void execute(Runnable command) {
        if (!live) throw new RejectedExecutionException("Executor has been shutdown!");
//        try {
        if (!queue.offer(new WorkerTask(command)/*, backlogInterval, TimeUnit.MILLISECONDS*/)) {
            throw new RejectedExecutionException("Executor reached to max capacity!");
        }
//        } catch (InterruptedException e) {
//            throw new RejectedExecutionException(e);
//        }
    }

    public void start() {
        for (Thread thread : threads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
    }

    public void shutdown() {
        live = false;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        queue.clear();
        threads.clear();
    }

    private void addWorker(boolean start) {
        lock.lock();
        try {
            final Worker worker = new Worker();
            final Thread thread = threadFactory.newThread(worker);
            if (start) {
                thread.start();
            }
            activeThreadCount++;
            threads.add(thread);
        } finally {
            lock.unlock();
        }
    }

    private class Worker implements Runnable {
        public void run() {
            final Thread currentThread = Thread.currentThread();
            final long timeout = keepAliveMillis;
            final int spin = 1000;
//            final int park = 1000;
//            final int sleep = 10000;
            WorkerTask task = null;
loop:       while (!currentThread.isInterrupted() && live) {
                if (task != null) {
                    task.run();
                }

                int spinCounter = spin;
                while (spinCounter > 0) {
                    task = queue.poll();
                    if (task != null) {
                        task.run();
                        spinCounter = spin;
                    } else {
                        if (currentThread.isInterrupted()) {
                            return;
                        }
                        spinCounter--;
                    }
                }
                int parkCounter = 10000;
                while (parkCounter > 0) {
                    task = queue.poll();
                    if (task != null) {
                        continue loop;
                    } else {
                        if (currentThread.isInterrupted()) {
                            return;
                        }
                        LockSupport.parkNanos(1L);
                        parkCounter--;
                    }
                }
//                int sleepCounter = 10000;
//                while (sleepCounter > 0) {
//                    task = queue.poll();
//                    if (task != null) {
//                        continue loop;
//                    } else {
//                        try {
//                            Thread.sleep(1);
//                        } catch (InterruptedException e) {
//                            return;
//                        }
//                        sleepCounter--;
//                    }
//                }
//                int parkCounter = 1000;
//                while (parkCounter > 0) {
//                    task = queue.poll();
//                    if (task != null) {
//                        task.run();
//                        parkCounter = park;
//                    } else {
//                        if (currentThread.isInterrupted()) {
//                            return;
//                        }
//                        LockSupport.parkNanos(1L);
//                        parkCounter--;
//                    }
//                }
//                int sleepCounter = 10000;
//                while (sleepCounter > 0) {
//                    task = queue.poll();
//                    if (task != null) {
//                        task.run();
//                        sleepCounter = sleep;
//                    } else {
//                        try {
//                            Thread.sleep(1);
//                        } catch (InterruptedException e) {
//                            return;
//                        }
//                        sleepCounter--;
//                    }
//                }

                try {
                    lock.lockInterruptibly();
                    try {
                        signalWorker.await(timeout, TimeUnit.MILLISECONDS);
                        task = queue.poll();
                        System.err.println("DEBUG: Awaken from sleep -> " + currentThread.getName() + " TASK: " + task);
                        if (task == null) {
                            if (activeThreadCount > coreThreadSize || allowCoreThreadTimeout) {
                                threads.remove(currentThread);
                                activeThreadCount--;
                                final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                                if (workerInterceptor != null) {
                                    workerInterceptor.afterWorkerTerminate();
                                }
                                return;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class BacklogDetector implements Runnable {

        public void run() {
            long currentBacklogInterval = backlogInterval;
            final Thread thread = Thread.currentThread();
            int k = 0;
            while (!thread.isInterrupted() && live) {
                long sleep = 100;
                final WorkerTask task = queue.peek();
                if (task != null) {
                    final long now = Clock.currentTimeMillis();
                    if (task.creationTime + currentBacklogInterval < now) {
                        if (activeThreadCount < maxThreadSize) {
                            final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                            if (workerInterceptor != null) {
                                workerInterceptor.beforeWorkerStart();
                            }
                            addWorker(true);
                        }
                    } else if (task.creationTime + 50 < now) {
                        try {
                            lock.lockInterruptibly();
                        } catch (InterruptedException e) {
                            break;
                        }
                        try {
                            System.err.println("DEBUG: Signalling a worker thread!");
                            signalWorker.signal();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                try {
                    Thread.sleep(sleep);
                    if (k++ % 300 == 0) {
                        System.err.println("DEBUG: Current operation thread count-> " + activeThreadCount);
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private class WorkerTask implements Runnable {
        final long creationTime = Clock.currentTimeMillis();
        final Runnable task;

        private WorkerTask(Runnable task) {
            this.task = task;
        }

        public void run() {
            task.run();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("WorkerTask");
            sb.append("{creationTime=").append(creationTime);
            sb.append(", task=").append(task);
            sb.append('}');
            return sb.toString();
        }
    }

    public void setInterceptor(WorkerLifecycleInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public int getCoreThreadSize() {
        return coreThreadSize;
    }

    public int getMaxThreadSize() {
        return maxThreadSize;
    }

    public long getKeepAliveMillis() {
        return keepAliveMillis;
    }

    public int getActiveThreadCount() {
        return activeThreadCount;
    }
}
