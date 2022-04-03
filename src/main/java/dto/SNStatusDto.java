package dto;

import lombok.Data;

import java.io.Serializable;
@Data
public class SNStatusDto implements Serializable {
    private long timestamp;
    private String statusVal;
    private String sn;

    public SNStatusDto(){}
    public SNStatusDto(long timestamp, String statusVal, String sn) {
        this.timestamp = timestamp;
        this.statusVal = statusVal;
        this.sn = sn;
    }

    @Override
    public String toString() {
        return "SNStatusDto{" +
                "timestamp=" + timestamp +
                ", statusVal='" + statusVal + '\'' +
                ", sn='" + sn + '\'' +
                '}';
    }
}
