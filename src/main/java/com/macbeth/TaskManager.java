package com.macbeth;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.macbeth.consumer.ConsumerTask;
import com.macbeth.producer.ProducerTask;
import jdk.nashorn.internal.objects.annotations.Getter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author chen
 * @date 2020-12-27
 */
public class TaskManager {
    private static Integer corePoolSize = Runtime.getRuntime().availableProcessors();
    private static Integer maxPoolSize = 2 * corePoolSize;
    private static Long keepAlive = 60L;
    private static TimeUnit timeUnit = TimeUnit.SECONDS;
    private static Integer queueSize = 50;
    private static BlockingQueue<Runnable> consumerQueue = new LinkedBlockingQueue<>(queueSize);
    private static ThreadPoolExecutor consumerThreadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive, timeUnit, consumerQueue, new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            System.out.println("消费者线程太多了");
        }
    });

    private static BlockingQueue<Runnable> producerQueue = new LinkedBlockingQueue<>(queueSize);
    private static ThreadPoolExecutor producerThreadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive, timeUnit, producerQueue, new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            System.out.println("生产者太多了");
        }
    });

    private static Map<String, ConsumerTaskGroup> consumerTaskGroupMap = Maps.newHashMap();
    private static Map<String, ProducerTaskGroup> producerTaskGroupMap = Maps.newHashMap();

    public static ProducerTaskGroup addProducerTask(String taskName, ProducerTask producerTask) {

        ProducerTaskGroup producerTaskGroup = null;
        if (Objects.nonNull(producerTaskGroup = producerTaskGroupMap.get(taskName))) {
            producerTaskGroup.addTask(producerTask);
        } else {
            producerTaskGroup = new ProducerTaskGroup();
            producerTaskGroupMap.put(taskName, producerTaskGroup);
            producerTaskGroup.addTask(producerTask);
        }
        return producerTaskGroup;
    }

    public static ConsumerTaskGroup addConsumerTask(String taskName, ConsumerTask consumerTask) {

        ConsumerTaskGroup consumerTaskGroup = null;
        if (Objects.nonNull(consumerTaskGroup = consumerTaskGroupMap.get(taskName))) {
            consumerTaskGroup.addTask(consumerTask);
        } else {
            consumerTaskGroup = new ConsumerTaskGroup();
            consumerTaskGroupMap.put(taskName, consumerTaskGroup);
            consumerTaskGroup.addTask(consumerTask);
        }
        return consumerTaskGroup;
    }

    public static class ProducerTaskGroup {
        private volatile Integer taskCount = 0;
        private volatile List<Future> futures = Lists.newArrayList();

        public Future addTask(ProducerTask producerTask) {
            final Future<?> future = TaskManager.producerThreadPool.submit(producerTask);
            this.futures.add(future);
            this.taskCount ++;
            return future;
        }

        public boolean isFinished() {
            final Iterator<Future> iterator = this.futures.iterator();
            boolean result = true;
            while (iterator.hasNext()) {
                final Future future = iterator.next();
                final boolean done = future.isDone();
                result = result && done;
                if (done) {
                    iterator.remove();
                    taskCount --;
                }
            }
            return result;
        }

        public List<Future> getFutures() {
            return this.futures;
        }
    }

    public static class ConsumerTaskGroup {
        private Integer taskCount = 0;
        private List<Future> futures = Lists.newArrayList();

        public Future addTask(ConsumerTask task) {
            taskCount ++;
            final Future<?> future = TaskManager.consumerThreadPool.submit(task);
            this.futures.add(future);
            return future;
        }

        public boolean isFinished() {
            final Iterator<Future> iterator = futures.iterator();
            boolean result = true;
            while (iterator.hasNext()) {
                final Future future = iterator.next();
                final boolean done = future.isDone();
                result = result && done;
                if (done) {
                    iterator.remove();
                    taskCount--;
                }
            }
            return result;
        }
    }
}
