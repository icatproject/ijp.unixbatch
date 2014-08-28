package org.icatproject.ijp.unixbatch.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class InternalException extends UnixBatchException {

	public InternalException(String message) {
		super(HttpURLConnection.HTTP_INTERNAL_ERROR, message);
	}

}
