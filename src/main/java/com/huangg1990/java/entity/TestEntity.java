package com.huangg1990.java.entity;

public class TestEntity {
    String id;
    String note;
    Long crate_time;
    Long upd_time;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public Long getCrate_time() {
        return crate_time;
    }

    public void setCrate_time(Long crate_time) {
        this.crate_time = crate_time;
    }

    public Long getUpd_time() {
        return upd_time;
    }

    public void setUpd_time(Long upd_time) {
        this.upd_time = upd_time;
    }

    @Override
    public String toString() {
        return "TestEntity{" +
                "id='" + id + '\'' +
                ", note='" + note + '\'' +
                ", crate_time=" + crate_time +
                ", upd_time=" + upd_time +
                '}';
    }
}
