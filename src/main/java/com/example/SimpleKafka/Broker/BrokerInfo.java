package com.example.SimpleKafka.Broker;

//Holds information about a Kafka broker, including its ID, host, and port in the SimpleKafka Cluster.

public class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "BrokerInfo{id=" + id + ", host='" + host + "', port=" + port + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BrokerInfo other = (BrokerInfo) obj;
        return id == other.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}
