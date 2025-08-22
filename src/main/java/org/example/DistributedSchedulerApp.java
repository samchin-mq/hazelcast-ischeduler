package org.example;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.DuplicateTaskException;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.TaskUtils;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DistributedSchedulerApp {

    public static void main(String[] args) {
        // Create a unique ID for this instance to easily identify it.
        String instanceId = "instance-" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("Starting Hazelcast instance with ID: " + instanceId);

        // Configure Hazelcast. We use a simple multicast setup for peer discovery,
        // which is a truly decentralized approach.
        Config config = new Config();
        config.setClusterName("distributed-scheduler-cluster");
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1");
        // Start a Hazelcast instance. This node automatically joins the cluster.
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        // Get the distributed scheduled executor service. The name "my-scheduler"
        // uniquely identifies this service across the cluster.
        IScheduledExecutorService scheduler = hazelcastInstance.getScheduledExecutorService("my-scheduler");
        IScheduledExecutorService scheduler2 = hazelcastInstance.getScheduledExecutorService("my-scheduler2");
        // The task to be scheduled. This must be a Serializable object.

        MyScheduledTask task = new MyScheduledTask(hazelcastInstance.getName());
        MySecondScheduledTask secondScheduledTask = new MySecondScheduledTask(hazelcastInstance.getName());

        System.out.println("Scheduling task 'my-periodic-task' to run every 5 seconds...");
        System.out.println("Scheduling task 'my-second-periodic-task' to run every 5 seconds...");
        // Schedule the task to run at a fixed rate.
        // Hazelcast ensures that this task is executed by only ONE member of the cluster.
        // It provides load balancing and failover automatically.
        try {
            scheduler.scheduleAtFixedRate(TaskUtils.named("my-periodic-task-1", task), 0, 5, TimeUnit.SECONDS);
            scheduler2.scheduleAtFixedRate(TaskUtils.named("my-periodic-task-2", secondScheduledTask), 0, 5, TimeUnit.SECONDS);
        } catch (DuplicateTaskException e) {
            System.out.println("Task was already scheduled by another member.");
        }
        // Keep the application running indefinitely.
//        System.out.println("Application is running. Press CTRL+C to stop.");
    }
}
