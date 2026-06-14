package pool;

public class RejectionPolicies {

    public static class AbortPolicy implements RejectedTaskHandler {
        @Override
        public void rejected(Runnable task, CustomThreadPool pool) {
            System.out.printf("[Rejected] Task '%s' was rejected due to overload! (AbortPolicy)%n",
                    task.toString());
            throw new RuntimeException("Task rejected: pool is full. Task: " + task);
        }
    }

    public static class DiscardPolicy implements RejectedTaskHandler {
        @Override
        public void rejected(Runnable task, CustomThreadPool pool) {
            System.out.printf("[Rejected] Task '%s' silently discarded. (DiscardPolicy)%n",
                    task.toString());
        }
    }

    public static class CallerRunsPolicy implements RejectedTaskHandler {
        @Override
        public void rejected(Runnable task, CustomThreadPool pool) {
            if (!pool.isShutdown()) {
                System.out.printf("[Rejected] Pool is full. Caller thread '%s' runs task '%s' itself. (CallerRunsPolicy)%n",
                        Thread.currentThread().getName(), task.toString());
                task.run();
            } else {
                System.out.printf("[Rejected] Pool is shutdown, task '%s' discarded. (CallerRunsPolicy)%n",
                        task.toString());
            }
        }
    }
}