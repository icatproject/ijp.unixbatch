package org.icatproject.ijp.unixbatch.exceptions;

import java.io.ByteArrayOutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

@Provider
public class UnixBatchExceptionMapper implements ExceptionMapper<UnixBatchException> {

	private static Logger logger = Logger.getLogger(UnixBatchExceptionMapper.class);

	@Override
	public Response toResponse(UnixBatchException e) {
		logger.info("Processing: " + e.getClass() + " " + e.getMessage());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos);
		gen.writeStartObject().write("code", e.getClass().getSimpleName())
				.write("message", e.getMessage());
		gen.writeEnd().close();
		return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
	}
}