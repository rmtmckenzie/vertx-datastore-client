package com.spotify.asyncdatastoreclient;

/**
 * Datastore count exception.
 * <p>
 * Extends {@code DatastoreException} with a count of the current
 * number of requests.
 */
public class DatastoreCountException extends DatastoreException {

    private int count;

    public DatastoreCountException(final int count, final String message) {
        super(message);
        this.count = count;
    }

    public DatastoreCountException(final int count, final int statusCode, final String message) {
        super(statusCode, message);
        this.count = count;
    }

    public DatastoreCountException(final int count, Throwable throwable) {
        super(throwable);
        this.count = count;
    }

}
