package pool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {

    private final String poolName;
    private final AtomicInteger threadCounter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable workerRunnable) {
        String threadName = poolName + "-worker-" + threadCounter.getAndIncrement();

        Thread thread = new Thread(() -> {
            try {
                workerRunnable.run();
            } finally {
                System.out.printf("[Worker] %s terminated.%n", Thread.currentThread().getName());
            }
        }, threadName);

        thread.setDaemon(false);

        System.out.printf("[ThreadFactory] Creating new thread: %s%n", threadName);
        return thread;
    }
}