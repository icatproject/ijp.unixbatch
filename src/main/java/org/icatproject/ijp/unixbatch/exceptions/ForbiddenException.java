package org.icatproject.ijp.unixbatch.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class ForbiddenException extends UnixBatchException {

	public ForbiddenException(String message) {
		super(HttpURLConnection.HTTP_FORBIDDEN, message);
	}

}
