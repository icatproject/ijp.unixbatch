package org.icatproject.ijp.unixbatch;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.icatproject.ijp.batch.BatchJson;
import org.icatproject.ijp.batch.exceptions.BatchException;

@Provider
public class BatchExceptionMapper implements ExceptionMapper<BatchException> {

	@Override
	public Response toResponse(BatchException e) {
		return BatchJson.batchExceptionError(e);
	}
}