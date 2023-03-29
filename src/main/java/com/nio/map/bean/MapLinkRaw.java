package com.nio.map.bean;

import java.io.Serializable;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class MapLinkRaw implements Serializable {

    private String vehicleId;       //车辆ID
    private String metaUuid;        //uuid
    private String oldResponse;    //原始response
    private String newResponse;    //重构response

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getMetaUuid() {
        return metaUuid;
    }

    public void setMetaUuid(String metaUuid) {
        this.metaUuid = metaUuid;
    }

    public String getOldResponse() {
        return oldResponse;
    }

    public void setOldResponse(String oldResponse) {
        this.oldResponse = oldResponse;
    }

    public String getNewResponse() {
        return newResponse;
    }

    public void setNewResponse(String newResponse) {
        this.newResponse = newResponse;
    }
}
