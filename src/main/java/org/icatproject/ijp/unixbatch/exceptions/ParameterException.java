package org.icatproject.ijp.unixbatch.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class ParameterException extends UnixBatchException {

	public ParameterException(String message) {
		super(HttpURLConnection.HTTP_BAD_REQUEST, message);
	}

}
