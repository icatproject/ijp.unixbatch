package org.icatproject.ijp.unixbatch.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class SessionException extends UnixBatchException {

	public SessionException(String message) {
		super(HttpURLConnection.HTTP_FORBIDDEN, message);
	}

}
