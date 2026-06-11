package org.foreverfzl.cloudchache.common.exception;

public class CoreException  extends CloudCacheException{
    public CoreException(String message) {
        super(message);
    }


    public CoreException(
            String message,
            Throwable cause
    ) {
        super(message,cause);
    }
}
