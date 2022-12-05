package com.flink.CTI;



public class PulseObject {

    private String indicator;
    private String type;
    private String created;
    private String content;


    public PulseObject() {
    }

    public PulseObject(String indicator, String type, String created, String content) {
        this.indicator = indicator;
        this.type = type;
        this.created = created;
        this.content = content;
    }

    public String getIndicator() {
        return indicator;
    }

    public void setIndicator(String indicator) {
        this.indicator = indicator;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
