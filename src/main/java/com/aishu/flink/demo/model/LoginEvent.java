package com.aishu.flink.demo.model;

public class LoginEvent {
    private String userId;
    private String ip;
    private String type;
    private String ruleId;

    public LoginEvent(String userId, String ip, String type, String ruleId) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
        this.ruleId = ruleId;
    }

    public String getRuleId(){
        return ruleId;
    }

    public String getUserId() {
        return userId;
    }

    public String getType() {
        return type;
    }

    public String getIp() {
        return ip;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}