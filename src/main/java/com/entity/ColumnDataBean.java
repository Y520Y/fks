package com.entity;

/**
 * description
 *
 * @author wdj on 2018/6/10
 */
public class ColumnDataBean extends BaseEntity {

    private Object value;

    private String type;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
