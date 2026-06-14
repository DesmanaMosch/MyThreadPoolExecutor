package pool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {

    private final String name;
    private final BlockingQueue<Runnable> queue;
    private final CustomThreadPool pool;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private volatile boolean running = true;

    public Worker(String name,
                  BlockingQueue<Runnable> queue,
                  CustomThreadPool pool,
                  long keepAliveTime,
                  TimeUnit timeUnit) {
        this.name = name;
        this.queue = queue;
        this.pool = pool;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            try {
                Runnable task = queue.poll(keepAliveTime, timeUnit);

                if (task != null) {
                    System.out.printf("[Worker] %s executes '%s'%n", name, task.toString());
                    try {
                        task.run();
                    } catch (Exception e) {
                        System.out.printf("[Worker] %s caught exception in task '%s': %s%n",
                                name, task.toString(), e.getMessage());
                    }
                } else {
                    System.out.printf("[Worker] %s idle timeout, checking if should stop...%n", name);

                    if (pool.canWorkerStop(this)) {
                        System.out.printf("[Worker] %s idle timeout, stopping.%n", name);
                        running = false;
                    }
                }

                if (pool.isShutdown() && queue.isEmpty()) {
                    running = false;
                }

            } catch (InterruptedException e) {
                System.out.printf("[Worker] %s was interrupted.%n", name);
                running = false;
            }
        }

        pool.onWorkerTerminated(this);
    }

    public String getName() {
        return name;
    }
}