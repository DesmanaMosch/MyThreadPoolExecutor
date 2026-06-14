package demo;

import pool.CustomThreadPool;
import pool.RejectionPolicies;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=".repeat(60));
        System.out.println("СЦЕНАРИЙ 1: Базовая работа пула");
        System.out.println("=".repeat(60));
        scenario1_basicWork();

        Thread.sleep(500);
        System.out.println("\n" + "=".repeat(60));
        System.out.println("СЦЕНАРИЙ 2: Перегрузка (политика CallerRunsPolicy)");
        System.out.println("=".repeat(60));
        scenario2_overload();

        Thread.sleep(500);
        System.out.println("\n" + "=".repeat(60));
        System.out.println("СЦЕНАРИЙ 3: Submit с Future");
        System.out.println("=".repeat(60));
        scenario3_futureResult();

        Thread.sleep(500);
        System.out.println("\n" + "=".repeat(60));
        System.out.println("СЦЕНАРИЙ 4: Перегрузка с AbortPolicy");
        System.out.println("=".repeat(60));
        scenario4_abortPolicy();
    }

    static void scenario1_basicWork() throws InterruptedException {

        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                3, TimeUnit.SECONDS,
                5,
                1,
                new RejectionPolicies.CallerRunsPolicy(),
                "MyPool"
        );

        System.out.println("\n--- Отправляем 5 задач ---");
        for (int i = 1; i <= 5; i++) {
            final int taskId = i;
            pool.execute(makeTask("Task-" + taskId, 500));
        }

        System.out.println("\n--- Ждём завершения и вызываем shutdown ---");
        Thread.sleep(3000);
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("--- Пул завершён ---");
    }

    static void scenario2_overload() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2, 2,
                2, TimeUnit.SECONDS,
                3, // queueSize
                0, // minSpareThreads
                new RejectionPolicies.CallerRunsPolicy(),
                "SmallPool"
        );

        System.out.println("\n--- Отправляем 15 задач в маленький пул ---");
        for (int i = 1; i <= 15; i++) {
            final int taskId = i;
            pool.execute(makeTask("HeavyTask-" + taskId, 800));
        }

        Thread.sleep(5000);
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("--- SmallPool завершён ---");
    }

    static void scenario3_futureResult() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2, 4,
                5, TimeUnit.SECONDS,
                10,
                1,
                new RejectionPolicies.CallerRunsPolicy(),
                "FuturePool"
        );

        System.out.println("\n--- Отправляем Callable задачи через submit() ---");

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            final int value = i * 10;
            Future<Integer> f = pool.submit(() -> {
                System.out.printf("[Task] Computing %d * 2 in thread %s%n",
                        value, Thread.currentThread().getName());
                Thread.sleep(300);
                return value * 2;
            });
            futures.add(f);
        }

        System.out.println("\n--- Собираем результаты ---");
        for (int i = 0; i < futures.size(); i++) {
            try {
                Integer result = futures.get(i).get(5, TimeUnit.SECONDS);
                System.out.printf("[Main] Future[%d] result = %d%n", i, result);
            } catch (ExecutionException e) {
                System.out.printf("[Main] Future[%d] threw exception: %s%n", i, e.getCause().getMessage());
            } catch (TimeoutException e) {
                System.out.printf("[Main] Future[%d] timed out!%n", i);
            }
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("--- FuturePool завершён ---");
    }

    static void scenario4_abortPolicy() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                1, 1,
                2, TimeUnit.SECONDS,
                2,
                0,
                new RejectionPolicies.AbortPolicy(),
                "AbortPool"
        );

        System.out.println("\n--- Пробуем переполнить пул с AbortPolicy ---");
        for (int i = 1; i <= 6; i++) {
            try {
                final int taskId = i;
                pool.execute(makeTask("AbortTask-" + taskId, 1000));
                System.out.printf("[Main] Task %d accepted%n", taskId);
            } catch (RuntimeException e) {
                System.out.printf("[Main] Caught rejection exception: %s%n", e.getMessage());
            }
        }

        Thread.sleep(5000);
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("--- AbortPool завершён ---");
    }

    static Runnable makeTask(String name, long sleepMs) {
        return new Runnable() {
            @Override
            public void run() {
                System.out.printf("[Task] %s started in %s%n", name, Thread.currentThread().getName());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    System.out.printf("[Task] %s was interrupted%n", name);
                    Thread.currentThread().interrupt();
                }
                System.out.printf("[Task] %s finished%n", name);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}