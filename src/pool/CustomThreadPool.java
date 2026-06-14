package pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {

    // ===== Параметры пула =====
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    // ===== Компоненты =====
    private final CustomThreadFactory threadFactory;
    private final RejectedTaskHandler rejectedHandler;

    // ===== Рабочие структуры =====
    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    // ===== Счётчики и флаги =====
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final AtomicInteger activeWorkerCount = new AtomicInteger(0);
    private volatile boolean shutdown = false;
    private volatile boolean shutdownNow = false;
    private final Object lock = new Object();

    // ===== Конструктор =====
    public CustomThreadPool(int corePoolSize,
                            int maxPoolSize,
                            long keepAliveTime,
                            TimeUnit timeUnit,
                            int queueSize,
                            int minSpareThreads,
                            RejectedTaskHandler rejectedHandler,
                            String poolName) {

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.rejectedHandler = rejectedHandler;
        this.threadFactory = new CustomThreadFactory(poolName);

        System.out.printf("[Pool] Initializing pool '%s' (core=%d, max=%d, queueSize=%d)%n",
                poolName, corePoolSize, maxPoolSize, queueSize);

        for (int i = 0; i < corePoolSize; i++) {
            addWorker(true);
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("Task cannot be null");

        if (shutdown) {
            System.out.printf("[Pool] Rejected '%s': pool is shut down.%n", command);
            rejectedHandler.rejected(command, this);
            return;
        }

        ensureMinSpareThreads();

        if (tryEnqueueRoundRobin(command)) {
            return;
        }

        synchronized (lock) {
            if (activeWorkerCount.get() < maxPoolSize) {
                Worker newWorker = addWorker(false);
                BlockingQueue<Runnable> newQueue = queues.get(queues.size() - 1);
                if (newQueue.offer(command)) {
                    System.out.printf("[Pool] Task accepted into new worker queue: '%s'%n", command);
                    return;
                }
            }
        }

        System.out.printf("[Pool] All queues full, triggering rejection for '%s'%n", command);
        rejectedHandler.rejected(command, this);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("Callable cannot be null");
        String taskName = "callable#" + roundRobinCounter.get();
        FutureTask<T> futureTask = new FutureTask<>(callable) {
            @Override
            public String toString() {
                return taskName;
            }
        };
        execute(futureTask);
        return futureTask;
    }

    // ===== Балансировка =====
    private boolean tryEnqueueRoundRobin(Runnable command) {
        synchronized (lock) {
            int size = queues.size();
            if (size == 0) return false;

            int startIndex = Math.abs(roundRobinCounter.getAndIncrement() % size);

            for (int attempt = 0; attempt < size; attempt++) {
                int index = (startIndex + attempt) % size;
                BlockingQueue<Runnable> queue = queues.get(index);

                if (queue.offer(command)) {
                    System.out.printf("[Pool] Task accepted into queue #%d: '%s'%n", index, command);
                    return true;
                }
            }
        }
        return false;
    }

    // ===== Управление воркерами =====
    private Worker addWorker(boolean isCore) {
        synchronized (lock) {

            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);

            Worker worker = new Worker(
                    "worker-" + (activeWorkerCount.get() + 1),
                    queue,
                    this,
                    isCore ? Long.MAX_VALUE : keepAliveTime,
                    isCore ? TimeUnit.MILLISECONDS : timeUnit
            );

            Thread thread = threadFactory.newThread(worker);

            workers.add(worker);
            queues.add(queue);
            threads.add(thread);
            activeWorkerCount.incrementAndGet();

            thread.start();
            return worker;
        }
    }

    private void ensureMinSpareThreads() {
        synchronized (lock) {
            if (shutdown) return;

            long spareCount = queues.stream().filter(q -> q.isEmpty()).count();

            int needed = (int)(minSpareThreads - spareCount);
            for (int i = 0; i < needed && activeWorkerCount.get() < maxPoolSize; i++) {
                System.out.printf("[Pool] Creating spare thread (spare=%d, needed=%d)%n",
                        spareCount, minSpareThreads);
                addWorker(false);
            }
        }
    }

    public boolean canWorkerStop(Worker worker) {
        synchronized (lock) {
            if (activeWorkerCount.get() > corePoolSize) {
                long spareCount = queues.stream().filter(BlockingQueue::isEmpty).count();
                if (spareCount > minSpareThreads) {
                    return true;
                }
            }
            return false;
        }
    }

    public void onWorkerTerminated(Worker worker) {
        synchronized (lock) {
            int index = workers.indexOf(worker);
            if (index >= 0) {
                workers.remove(index);
                queues.remove(index);
                threads.remove(index);
                activeWorkerCount.decrementAndGet();
                System.out.printf("[Pool] Worker removed from pool. Active workers: %d%n",
                        activeWorkerCount.get());
                        lock.notifyAll();
            }
        }
    }

    // ===== Управление жизненным циклом пула =====
    @Override
    public void shutdown() {
        System.out.println("[Pool] Initiating graceful shutdown...");
        shutdown = true;
        synchronized (lock) {
            for (Thread t : threads) {
                t.interrupt();
            }
        }
    }

    @Override
    public void shutdownNow() {
        System.out.println("[Pool] Initiating immediate shutdown...");
        shutdown = true;
        shutdownNow = true;
        synchronized (lock) {
            for (Worker w : workers) {
                w.stop();
            }
            for (Thread t : threads) {
                t.interrupt();
            }
        }
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        synchronized (lock) {
            while (!threads.isEmpty()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;

                lock.wait(remaining);
            }
        }

        List<Thread> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(threads);
        }
        for (Thread t : snapshot) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            t.join(remaining);
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public int getActiveWorkerCount() {
        return activeWorkerCount.get();
    }

    public int getQueueSizes() {
        synchronized (lock) {
            return queues.stream().mapToInt(BlockingQueue::size).sum();
        }
    }
}