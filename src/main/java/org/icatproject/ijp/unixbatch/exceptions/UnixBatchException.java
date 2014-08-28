package org.icatproject.ijp.unixbatch.exceptions;

@SuppressWarnings("serial")
public class UnixBatchException extends Exception {

	private int httpStatusCode;

	public UnixBatchException(int httpStatusCode, String message) {
		super(message);
		this.httpStatusCode = httpStatusCode;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

}
