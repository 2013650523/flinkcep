package cep;

public class LogInEvent {
    private Long Timestamp;
    private String Action;
    private Long uId;

    public Long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(Long timestamp) {
        Timestamp = timestamp;
    }

    public Long getuId() {
        return uId;
    }

    public void setuId(Long uId) {
        this.uId = uId;
    }

    public String getAction() {
        return Action;
    }

    public void setAction(String action) {
        Action = action;
    }

    public LogInEvent(Long uId, String action, Long timestamp) {
        Timestamp = timestamp;
        Action = action;
        this.uId = uId;
    }

    @Override
    public String toString() {
        return "LogInEvent{" +
                "Timestamp=" + Timestamp +
                ", uId=" + uId +
                ", Action='" + Action + '\'' +
                '}';
    }
}
