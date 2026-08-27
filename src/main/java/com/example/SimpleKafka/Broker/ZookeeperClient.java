package com.example.SimpleKafka.Broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;
    private static final int CONNECT_TIMEOUT_SECONDS = 10;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal;

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.connectedSignal = new CountDownLatch(1);
    }

    /**
     * Connect to ZooKeeper and initialize base cluster paths
     */
    public synchronized void connect() throws IOException, InterruptedException {
        this.connectedSignal = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);

        if (!connectedSignal.await(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for ZooKeeper connection to " + getConnectString());
        }

        // Initialize required root paths
        createPath("/brokers");
        createPath("/brokers/ids");
        createPath("/topics");
        createPath("/controller");
    }

    public String getConnectString() {
        return host + ":" + port;
    }

    public synchronized void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
            zooKeeper = null;
        }
    }

    /**
     * Creates or updates a persistent node safely
     */
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        byte[] bytes = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0];
        try {
            zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } catch (KeeperException.NodeExistsException e) {
            zooKeeper.setData(path, bytes, -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /**
     * Atomic ephemeral node creation for leader/controller election
     */
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        byte[] bytes = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0];
        try {
            zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, false) != null;
    }

    public String getData(String path) throws KeeperException, InterruptedException {
        try {
            byte[] data = zooKeeper.getData(path, false, null);
            return data != null ? new String(data, StandardCharsets.UTF_8) : "";
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        byte[] bytes = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0];
        zooKeeper.setData(path, bytes, -1);
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    /**
     * Recursively creates path hierarchy
     */
    public void createPath(String path) {
        if (path == null || path.equals("/") || path.isEmpty()) {
            return;
        }

        int lastSlashIndex = path.lastIndexOf('/');
        if (lastSlashIndex > 0) {
            createPath(path.substring(0, lastSlashIndex));
        }

        try {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created ZooKeeper path: " + path);
        } catch (KeeperException.NodeExistsException ignored) {
            // Path already exists
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }

    /**
     * Persistent watcher for child node topology changes (e.g. /brokers/ids)
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    // Re-register watch on subsequent updates
                    watchChildren(path, callback);
                }
            });
            callback.onChildrenChanged(children);
        } catch (KeeperException.NoNodeException e) {
            LOGGER.log(Level.WARNING, "Cannot watch non-existent path: " + path);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    /**
     * Persistent watcher for specific node changes (e.g. /controller)
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            Stat stat = zooKeeper.exists(path, event -> {
                // Re-register the watch upon firing
                watchNode(path, callback);
                callback.onNodeChanged();
            });

            if (stat == null) {
                // Node does not exist yet; watch for creation
                zooKeeper.exists(path, event -> {
                    watchNode(path, callback);
                    callback.onNodeChanged();
                });
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {
        try {
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted node: " + path);
        } catch (KeeperException.NoNodeException ignored) {
            // Already deleted
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired. Reconnecting in background thread...");
            // Run reconnection in a separate thread to prevent blocking EventThread
            new Thread(() -> {
                try {
                    close();
                    connect();
                    LOGGER.info("Successfully reconnected to ZooKeeper after session expiry");
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
                }
            }, "zk-reconnect-thread").start();
        }
    }

    @FunctionalInterface
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    @FunctionalInterface
    public interface NodeCallback {
        void onNodeChanged();
    }
}