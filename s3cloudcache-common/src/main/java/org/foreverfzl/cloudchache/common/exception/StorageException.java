package org.foreverfzl.cloudchache.common.exception;

public class StorageException extends CloudCacheException{

    public StorageException(String message) {
        super(message);
    }


    public StorageException(String message, Throwable cause) {
        super(message,cause);
    }
}
