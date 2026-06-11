package org.foreverfzl.cloudchache.common.exception;

public class CloudCacheException extends RuntimeException {
    public CloudCacheException() {
        super();
    }


    public CloudCacheException(String message) {
        super(message);
    }


    public CloudCacheException(String message, Throwable cause) {
        super(message, cause);
    }


    public CloudCacheException(Throwable cause) {
        super(cause);
    }
}
