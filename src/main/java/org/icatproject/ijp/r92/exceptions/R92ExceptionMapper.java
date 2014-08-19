package org.icatproject.ijp.r92.exceptions;

import java.io.ByteArrayOutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

@Provider
public class R92ExceptionMapper implements ExceptionMapper<R92Exception> {

	private static Logger logger = Logger.getLogger(R92ExceptionMapper.class);

	@Override
	public Response toResponse(R92Exception e) {

		logger.info("Processing: " + e.getClass() + " " + e.getMessage());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos);
		gen.writeStartObject().write("code", e.getClass().getSimpleName())
				.write("message", e.getMessage());
		gen.writeEnd().close();
		return Response.ok().entity(baos.toString()).build();

	}
}