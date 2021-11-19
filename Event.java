package cep;

public class Event {
    private Long Id;
    private String Name;
    private Long Volume;

    public Event(Long id, String name, Long volume) {
        Id = id;
        Name = name;
        Volume = volume;
    }

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public Long getVolume() {
        return Volume;
    }

    public void setVolume(Long volume) {
        Volume = volume;
    }

    @Override
    public String toString() {
        return "Event{" +
                "Id=" + Id +
                ", Name='" + Name + '\'' +
                ", Volume=" + Volume +
                '}';
    }
}
