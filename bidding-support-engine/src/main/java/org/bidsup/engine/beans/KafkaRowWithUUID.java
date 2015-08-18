package org.bidsup.engine.beans;

import java.io.Serializable;
import java.util.UUID;

public class KafkaRowWithUUID implements Serializable {
    private UUID key;
    private String value;

    // Remember to declare no-args constructor
    public KafkaRowWithUUID() { }

    public KafkaRowWithUUID(UUID key, String value) {
        this.key = key;
        this.value = value;
    }

    public UUID getKey() {
        return key;
    }

    public void setKey(UUID key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KafkaRowWithUUID{" +
                "key=" + key +
                ", value='" + value + '\'' +
                '}';
    }
    // other methods, constructors, etc.
}
