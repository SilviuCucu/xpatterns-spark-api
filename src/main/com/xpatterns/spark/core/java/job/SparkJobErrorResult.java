package com.xpatterns.spark.core.java.job;

import java.util.Arrays;


/**
 * Created by radum on 10.04.2014.
 */

public class SparkJobErrorResult {

    private String message;
    private String errorClass;
    private String[] stack;
    private String cause;
    private String causingClass;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public void setErrorClass(String errorClass) {
        this.errorClass = errorClass;
    }

    public String[] getStack() {
        return stack;
    }

    public void setStack(String[] stack) {
        this.stack = stack;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public String getCausingClass() {
        return causingClass;
    }

    public void setCausingClass(String causingClass) {
        this.causingClass = causingClass;
    }

    @Override
    public String toString() {
        return "SparkJobResult [message=" + message + ", errorClass="
                + errorClass + ", stack=" + Arrays.toString(stack) + ", cause="
                + cause + ", causingClass=" + causingClass + "]";
    }

}
