package dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class ClockDto implements Serializable {
    private Long timestamp;
    private String statusVal;
    private String sn;

    public ClockDto(Long timestamp, String statusVal, String sn) {
        this.timestamp = timestamp;
        this.statusVal = statusVal;
        this.sn = sn;
    }

    public ClockDto(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ClockDto(String statusVal, String sn) {
        this.statusVal = statusVal;
        this.sn = sn;
    }
}
