package org.icatproject.ijp.r92;

import java.io.InputStream;
import java.util.List;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.icatproject.ijp.r92.JobManagementBean.OutputType;
import org.icatproject.ijp.r92.exceptions.ForbiddenException;
import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.ijp.r92.exceptions.ParameterException;
import org.icatproject.ijp.r92.exceptions.SessionException;

@Stateless
public class JobManager {

	@EJB
	private JobManagementBean jobManagementBean;

	@POST
	@Path("cancel/{jobId}")
	/**
	 * Cancel the specified job if permitted to do so
	 *  
	 * @param jobId as returned by the call to submit
	 * @param sessionId the icatSession id of the submitter
	 * 
	 * @throws SessionException
	 * @throws ForbiddenException
	 * @throws InternalException
	 */
	public void cancel(@PathParam("jobId") String jobId, @QueryParam("sessionId") String sessionId)
			throws SessionException, ForbiddenException, InternalException {
		jobManagementBean.cancel(sessionId, jobId);
	}

	@DELETE
	@Path("delete/{jobId}")
	/**
	 * Delete all information on the specified job if permitted to do so and if it has completed
	 * 
	 * @param jobId as returned by the call to submit
	 * @param sessionId the icatSession id of the submitter
	 * 
	 * @throws SessionException
	 * @throws ForbiddenException
	 * @throws InternalException
	 * @throws ParameterException
	 */
	public void delete(@PathParam("jobId") String jobId, @QueryParam("sessionId") String sessionId)
			throws SessionException, ForbiddenException, InternalException, ParameterException {
		jobManagementBean.delete(sessionId, jobId);
	}

	@GET
	@Path("error/{jobId}")
	/**
	 * Stream the contents of the jobs standard standard error. If the job has not  
	 * finished running the output will be incomplete.
	 * 
	 * @param jobId as returned by the call to submit
	 * @param sessionId the icatSession id of the submitter
	 * 
	 * @return stream
	 * 
	 * @throws SessionException
	 * @throws ForbiddenException
	 * @throws InternalException
	 */
	public InputStream getError(@PathParam("jobId") String jobId,
			@QueryParam("sessionId") String sessionId) throws SessionException, ForbiddenException,
			InternalException {
		return jobManagementBean.getJobOutput(sessionId, jobId, OutputType.ERROR_OUTPUT);
	}

	@GET
	@Path("output/{jobId}")
	/**
	 * Stream the contents of the jobs standard standard output. If the job has not  
	 * finished running the output will be incomplete.
	 * 
	 * @param jobId as returned by the call to submit
	 * @param sessionId the icatSession id of the submitter
	 * 
	 * @return stream
	 * 
	 * @throws SessionException
	 * @throws ForbiddenException
	 * @throws InternalException
	 */
	public InputStream getOutput(@PathParam("jobId") String jobId,
			@QueryParam("sessionId") String sessionId) throws SessionException, ForbiddenException,
			InternalException {
		return jobManagementBean.getJobOutput(sessionId, jobId, OutputType.STANDARD_OUTPUT);

	}

	@GET
	@Path("status")
	/** Get the list of known jobs that may be queried by the user identified by the sessionId */
	public String getStatus(@QueryParam("sessionId") String sessionId) throws SessionException {
		return jobManagementBean.listStatus(sessionId);
	}

	@GET
	@Path("status/{jobId}")
	/**
	 * Get the status of a specific job
	 *  
	 * @param jobId as returned by the call to submit
	 * @param sessionId the icatSession id of the submitter
	 * 
	 * @return json holding
	 * 
	 * @throws SessionException
	 * @throws ForbiddenException
	 */
	public String getStatus(@PathParam("jobId") String jobId,
			@QueryParam("sessionId") String sessionId) throws SessionException, ForbiddenException {
		return jobManagementBean.getStatus(jobId, sessionId);
	}

	@POST
	@Path("submit")
	/**
	 * Submit a job
	 * 
	 * @param sessionId the icatSession id of the submitter
	 * @param executable the executable name
	 * @param parameters the executables parameters
	 * @param interactive true if interactive else false
	 * @param family the name of the family. A family identifies a group of user accounts. If omitted the default family can be used.
	 *  
	 * @return The job id - this could be the id assigned by the underlying batch system.
	 * 
	 * @throws InternalException
	 * @throws SessionException
	 * @throws ParameterException
	 */
	public String submit(@QueryParam("sessionId") String sessionId,
			@QueryParam("executable") String executable,
			@QueryParam("parameter") List<String> parameters,
			@QueryParam("interactive") boolean interactive, @QueryParam("family") String family)
			throws InternalException, SessionException, ParameterException {
		return jobManagementBean.submit(sessionId, executable, parameters, family, interactive);
	}

}