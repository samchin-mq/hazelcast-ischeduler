package org.example;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class MySecondScheduledTask implements Runnable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String schedulerInstanceId;

    public MySecondScheduledTask(String schedulerInstanceId) {
        this.schedulerInstanceId = schedulerInstanceId;
    }

    @Override
    public void run() {
        LocalTime now = LocalTime.now();

        // Define a pattern for the format
        // HH:mm:ss for 24-hour format
        // hh:mm:ss a for 12-hour format with AM/PM
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        // Format the time
        String formattedTime = now.format(formatter);
        // This is the task's logic. It will be executed on a cluster member.
        // Hazelcast guarantees that only one member executes it at any given time.
        System.out.println("Task 'my-periodic-task-2' executed successfully by " + schedulerInstanceId + " at " + formattedTime);
    }
}
