package org.foreverfzl.cloudchache.common.exception;

public class WalException extends CloudCacheException {
    public WalException(String message) {
        super(message);
    }


    public WalException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
