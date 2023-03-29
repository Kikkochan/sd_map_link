package com.nio.map.bean;

import java.io.Serializable;
import java.util.List;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class NewResponse implements Serializable {

    private Long linkId;             // 当前的link id
    private Long preLinkId;          // 上一个 link ID
    private Long nextLinkId;         // 下一个link id

    private Integer isReverse;       //是否反向
    private List timestamp;        //时间戳
    private List offset;             //偏移量
    private Integer reliability;     //置信度

    public Long getLinkId() {
        return linkId;
    }

    public void setLinkId(Long linkId) {
        this.linkId = linkId;
    }

    public Long getPreLinkId() {
        return preLinkId;
    }

    public void setPreLinkId(Long preLinkId) {
        this.preLinkId = preLinkId;
    }

    public Long getNextLinkId() {
        return nextLinkId;
    }

    public void setNextLinkId(Long nextLinkId) {
        this.nextLinkId = nextLinkId;
    }

    public Integer getIsReverse() {
        return isReverse;
    }

    public void setIsReverse(Integer isReverse) {
        this.isReverse = isReverse;
    }

    public List getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(List timestamp) {
        this.timestamp = timestamp;
    }

    public List getOffset() {
        return offset;
    }

    public void setOffset(List offset) {
        this.offset = offset;
    }

    public Integer getReliability() {
        return reliability;
    }

    public void setReliability(Integer reliability) {
        this.reliability = reliability;
    }
}
