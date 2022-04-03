package dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class SNDisconnectedDto implements Serializable {
    //在线的时间戳
    private Long OnlineEventTimestamp;

    //离线时间戳
    private Long OffEventTimestamp;

    private String sn;
    public SNDisconnectedDto(){}
    public SNDisconnectedDto(Long onlineEventTimestamp, Long offEventTimestamp) {
        OnlineEventTimestamp = onlineEventTimestamp;
        OffEventTimestamp = offEventTimestamp;
    }

}
