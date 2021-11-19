package cep;

import lombok.Data;

@Data
public class PayEvent {
    private Long Id;
    private String Name;
    private Long Volume;

    public PayEvent(Long id, String name, Long volume) {
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
}
