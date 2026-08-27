package com.example.SimpleKafka;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.util.Properties;

public class EmbeddedZooKeeper {
    public static void main(String[] args) {
        try {
            // Disable Jetty admin server and snapshot compression
            System.setProperty("zookeeper.admin.enableServer", "false");
            System.setProperty("zookeeper.snapshot.compression.method", "");

            Properties startupProperties = new Properties();
            File zkDir = new File(System.getProperty("java.io.tmpdir"), "zookeeper-local-" + System.currentTimeMillis());
            zkDir.deleteOnExit();

            startupProperties.put("dataDir", zkDir.getAbsolutePath());
            startupProperties.put("clientPort", "2181");
            startupProperties.put("tickTime", "2000");
            startupProperties.put("metricsProvider.className", "org.apache.zookeeper.metrics.impl.NullMetricsProvider");

            QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
            quorumConfig.parseProperties(startupProperties);

            ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.readFrom(quorumConfig);

            System.out.println("=================================================");
            System.out.println(" Starting Embedded ZooKeeper on localhost:2181  ");
            System.out.println(" Data directory: " + zkDir.getAbsolutePath());
            System.out.println("=================================================");

            zkServer.runFromConfig(serverConfig);
        } catch (Exception e) {
            System.err.println("Failed to start ZooKeeper: " + e.getMessage());
            e.printStackTrace();
        }
    }
}