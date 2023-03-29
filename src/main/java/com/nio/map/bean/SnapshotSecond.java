package com.nio.map.bean;

import java.io.Serializable;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class SnapshotSecond implements Serializable {

    private String vehicleId;
    private String metaUuid;
    private Integer ts;
    private String tsf;
    private Double longitude;
    private Double latitude;
    private Double vehspdkph;
    private Double yaw;

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

    public Integer getTs() {
        return ts;
    }

    public void setTs(Integer ts) {
        this.ts = ts;
    }

    public String getTsf() {
        return tsf;
    }

    public void setTsf(String tsf) {
        this.tsf = tsf;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getVehspdkph() {
        return vehspdkph;
    }

    public void setVehspdkph(Double vehspdkph) {
        this.vehspdkph = vehspdkph;
    }

    public Double getYaw() {
        return yaw;
    }

    public void setYaw(Double yaw) {
        this.yaw = yaw;
    }
}
