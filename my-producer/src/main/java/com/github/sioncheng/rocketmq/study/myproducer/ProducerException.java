package com.github.sioncheng.rocketmq.study.myproducer;

public class ProducerException extends Exception {

    public static enum  ExceptionCode {
        FETCH_CLUSTER_INFO_FAILURE
    }

    private ExceptionCode code;

    public ExceptionCode getCode() {
        return code;
    }

    public ProducerException(ExceptionCode code, String message) {
        this(code, message, null);
    }

    public ProducerException(ExceptionCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
}
