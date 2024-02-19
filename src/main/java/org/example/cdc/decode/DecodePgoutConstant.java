package org.example.cdc.decode;


public enum DecodePgoutConstant {

    NEW_VALUE_REPLACED("NEW_VALUE_REPLACED"),
    UPDATE_NEW_VALUE_SEQUENCE("N\u0000\u0005");

    private String value;

    DecodePgoutConstant(String s) {
        this.value = s;
    }

    public String getValue() {
        return value;
    }
}
