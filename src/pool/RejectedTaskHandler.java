package pool;

public interface RejectedTaskHandler {

    void rejected(Runnable task, CustomThreadPool pool);
}