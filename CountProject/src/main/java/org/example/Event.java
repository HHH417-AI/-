package org.example;

public class Event {
    private String name;
    private Long timestamp;

    public Event() {
    }

    public Event(String name, Long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
