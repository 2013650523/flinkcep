package cep;

public class AlertEvent {
    private String id;
    private String name;

    public AlertEvent(String id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "AlertEvent{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
