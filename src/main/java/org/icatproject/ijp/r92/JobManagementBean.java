package org.icatproject.ijp.r92;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.ijp.r92.exceptions.ForbiddenException;
import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.ijp.r92.exceptions.ParameterException;
import org.icatproject.ijp.r92.exceptions.SessionException;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session Bean implementation to manage job status
 */
@Stateless
public class JobManagementBean {

	public enum OutputType {
		STANDARD_OUTPUT, ERROR_OUTPUT;
	}

	private ICAT icat;

	private class LocalFamily {

		private int count;
		private Pattern re;

		public LocalFamily(int count, Pattern re) {
			this.count = count;
			this.re = re;
		}

	}

	@EJB
	private MachineEJB machineEJB;

	private String defaultFamily;
	private Map<String, LocalFamily> families = new HashMap<String, LocalFamily>();
	private Unmarshaller qstatUnmarshaller;

	@PostConstruct
	void init() {

		// try {
		// XmlFileManager xmlFileManager = new XmlFileManager();
		// jobTypes = xmlFileManager.getJobTypeMappings().getJobTypesMap();
		// logger.debug("Initialised JobManager");
		// } catch (Exception e) {
		// String msg = e.getClass().getName() + " reports " + e.getMessage();
		// logger.error(msg);
		// throw new RuntimeException(msg);

		// icat = Icat.getIcat();
		//
		// Unmarshaller um = JAXBContext.newInstance(Families.class).createUnmarshaller();
		// Families fams = (Families) um.unmarshal(new FileReader(Constants.CONFIG_SUBDIR
		// + "/families.xml"));
		// PrintStream p = new PrintStream("/etc/puppet/modules/usergen/manifests/init.pp");
		// p.print(fams.getPuppet());
		// p.close();
		// defaultFamily = fams.getDefault();
		// for (Families.Family fam : fams.getFamily()) {
		// LocalFamily lf = new LocalFamily(fam.getCount(), fam.getRE());
		// families.put(fam.getName(), lf);
		// }
		//
		// qstatUnmarshaller = JAXBContext.newInstance(Qstat.class).createUnmarshaller();
		//
		// XmlFileManager xmlFileManager = new XmlFileManager();
		// jobTypes = xmlFileManager.getJobTypeMappings().getJobTypesMap();
		//
		// logger.debug("Initialised JobManagementBean");
		// } catch (Exception e) {
		// String msg = e.getClass().getName() + " reports " + e.getMessage();
		// logger.error(msg);
		// throw new RuntimeException(msg);
		// }
	}

	private final static Logger logger = LoggerFactory.getLogger(JobManagementBean.class);
	private final static Random random = new Random();
	private final static String chars = "abcdefghijklmnpqrstuvwxyz";

	@PersistenceContext(unitName = "r92")
	private EntityManager entityManager;

	public List<Job> getJobsForUser(String sessionId) throws SessionException {
		String username = getUserName(sessionId);
		return entityManager.createNamedQuery(Job.FIND_BY_USERNAME, Job.class)
				.setParameter("username", username).getResultList();
	}

	public InputStream getJobOutput(String sessionId, String jobId, OutputType outputType)
			throws SessionException, ForbiddenException, InternalException {
		Job job = getJob(sessionId, jobId);
		String ext = "." + (outputType == OutputType.STANDARD_OUTPUT ? "o" : "e")
				+ jobId.split("\\.")[0];
		Path path = FileSystems.getDefault().getPath("/home/batch/jobs",
				job.getBatchFilename() + ext);
		if (!Files.exists(path)) {
			logger.debug("Getting intermediate output for " + jobId);
			ShellCommand sc = new ShellCommand("sudo", "-u", "batch", "ssh", job.getWorkerNode(),
					"sudo", "push_output", job.getBatchUsername(), path.toFile().getName());
			if (sc.isError()) {
				throw new InternalException("Temporary? problem getting output " + sc.getStderr());
			}
			path = FileSystems.getDefault().getPath("/home/batch/jobs",
					job.getBatchFilename() + ext + "_tmp");
		}
		if (Files.exists(path)) {
			logger.debug("Returning output for " + jobId);
			try {
				return Files.newInputStream(path);
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " reports " + e.getMessage());
			}
		} else {
			throw new InternalException("No output file available at the moment");
		}
	}

	@Schedule(minute = "*/1", hour = "*")
	public void updateJobsFromQstat() {
		try {

			ShellCommand sc = new ShellCommand("qstat", "-x");
			if (sc.isError()) {
				throw new InternalException("Unable to query jobs via qstat " + sc.getStderr());
			}
			String jobsXml = sc.getStdout().trim();
			if (jobsXml.isEmpty()) {
				/* See if any jobs have completed without being noticed */
				for (Job job : entityManager.createNamedQuery(Job.FIND_INCOMPLETE, Job.class)
						.getResultList()) {
					logger.warn("Updating status of job '" + job.getId() + "' from '"
							+ job.getStatus() + "' to 'C' as not known to qstat");
					job.setStatus("C");
				}
				return;
			}

			Qstat qstat = (Qstat) qstatUnmarshaller.unmarshal(new StringReader(jobsXml));
			for (Qstat.Job xjob : qstat.getJobs()) {
				String id = xjob.getJobId();
				String status = xjob.getStatus();
				String wn = xjob.getWorkerNode();
				String workerNode = wn != null ? wn.split("/")[0] : "";
				String comment = xjob.getComment() == null ? "" : xjob.getComment();

				Job job = entityManager.find(Job.class, id);
				if (job != null) {/* Log updates on portal jobs */
					if (!job.getStatus().equals(xjob.getStatus())) {
						logger.debug("Updating status of job '" + id + "' from '" + job.getStatus()
								+ "' to '" + status + "'");
						job.setStatus(status);
					}
					if (!job.getWorkerNode().equals(workerNode)) {
						logger.debug("Updating worker node of job '" + id + "' from '"
								+ job.getWorkerNode() + "' to '" + workerNode + "'");
						job.setWorkerNode(workerNode);
					}
					String oldComment = job.getComment() == null ? "" : job.getComment();
					if (!oldComment.equals(comment)) {
						logger.debug("Updating comment of job '" + id + "' from '" + oldComment
								+ "' to '" + comment + "'");
						job.setComment(comment);
					}
				}
			}
		} catch (Exception e) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.printStackTrace(new PrintStream(baos));
			logger.error("Update of db jobs from qstat failed. Class " + e.getClass() + " reports "
					+ e.getMessage() + baos.toString());
		}
	}

	public String submitBatch(String sessionId, String executable, List<String> parameters,
			String family) throws ParameterException, InternalException, SessionException {

		String username = getUserName(sessionId);

		// if (lf.re != null && !lf.re.matcher(username).matches()) {
		// throw new ParameterException(username + " is not allowed to use family " + family);
		// }
		String owner = family /* + random.nextInt(lf.count) */;

		/*
		 * The batch script needs to be written to disk by the dmf user (running glassfish) before
		 * it can be submitted via the qsub command as a less privileged batch user. First generate
		 * a unique name for it.
		 */
		File batchScriptFile = null;
		do {
			char[] pw = new char[10];
			for (int i = 0; i < pw.length; i++) {
				pw[i] = chars.charAt(random.nextInt(chars.length()));
			}
			String batchScriptName = new String(pw) + ".sh";
			batchScriptFile = new File(Constants.DMF_WORKING_DIR_NAME, batchScriptName);
		} while (batchScriptFile.exists());

		createScript(batchScriptFile, parameters, executable);

		ShellCommand sc = new ShellCommand("sudo", "-u", owner, "qsub", "-k", "eo",
				batchScriptFile.getAbsolutePath());
		if (sc.isError()) {
			throw new InternalException("Unable to submit job via qsub " + sc.getStderr());
		}
		String jobId = sc.getStdout().trim();

		sc = new ShellCommand("qstat", "-x", jobId);
		if (sc.isError()) {
			throw new InternalException("Unable to query just submitted job (id " + jobId
					+ ") via qstat " + sc.getStderr());
		}
		String jobsXml = sc.getStdout().trim();

		Qstat qstat;
		try {
			qstat = (Qstat) qstatUnmarshaller.unmarshal(new StringReader(jobsXml));
		} catch (JAXBException e1) {
			throw new InternalException("Unable to parse qstat output for job (id " + jobId + ") "
					+ sc.getStderr());
		}
		for (Qstat.Job xjob : qstat.getJobs()) {
			String id = xjob.getJobId();
			if (id.equals(jobId)) {
				Job job = new Job();
				job.setId(jobId);
				job.setStatus(xjob.getStatus());
				job.setComment(xjob.getComment());
				String wn = xjob.getWorkerNode();
				job.setWorkerNode(wn != null ? wn.split("/")[0] : "");
				job.setBatchUsername(owner);
				job.setUsername(username);
				job.setSubmitDate(new Date());
				job.setBatchFilename(batchScriptFile.getName());
				entityManager.persist(job);
			}
		}
		return jobId;
	}

	private void createScript(File batchScriptFile, List<String> parameters, String executable)
			throws InternalException {

		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(batchScriptFile));
			bw.write("#!/bin/sh");
			bw.newLine();
			bw.write("echo $(date) - " + executable + " starting");
			bw.newLine();
			String line = executable + " " + JobManagementBean.escaped(parameters);
			logger.debug("Exec line for " + executable + ": " + line);
			bw.write(line);
			bw.newLine();
			bw.newLine();
		} catch (IOException e) {
			throw new InternalException("Exception creating batch script: " + e.getMessage());
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					// Ignore it
				}
			}
		}
		batchScriptFile.setExecutable(true);

	}

	private static String sq = "\"'\"";

	static String escaped(List<String> parameters) {
		StringBuilder sb = new StringBuilder();
		for (String parameter : parameters) {
			if (sb.length() != 0) {
				sb.append(" ");
			}
			int offset = 0;
			while (true) {
				int quote = parameter.indexOf('\'', offset);
				if (quote == offset) {
					sb.append(sq);
				} else if (quote > offset) {
					sb.append("'" + parameter.substring(offset, quote) + "'" + sq);
				} else if (offset != parameter.length()) {
					sb.append("'" + parameter.substring(offset) + "'");
					break;
				} else {
					break;
				}
				offset = quote + 1;
			}
		}
		return sb.toString();
	}

	public String submitInteractive(String sessionId, String executable, List<String> parameters,
			String family) throws InternalException {
		Path p = null;
		try {
			p = Files.createTempFile(null, null);
		} catch (IOException e) {
			throw new InternalException("Unable to create a temporary file: " + e.getMessage());
		}
		File interactiveScriptFile = p.toFile();
		createScript(interactiveScriptFile, parameters, executable);
		Account account = machineEJB.prepareMachine(sessionId, executable, parameters,
				interactiveScriptFile);
		return account.getUserName() + " " + account.getPassword() + " " + account.getHost();
	}

	private String getUserName(String sessionId) throws SessionException {
		try {
			checkCredentials(sessionId);
			return icat.getUserName(sessionId);
		} catch (IcatException_Exception e) {
			throw new SessionException("IcatException " + e.getFaultInfo().getType() + " "
					+ e.getMessage());
		}
	}

	public String listStatus(String sessionId) throws SessionException {
		 String username = getUserName(sessionId);
		 List<Job> jobs = entityManager.createNamedQuery(Job.FIND_BY_USERNAME, Job.class)
		 .setParameter("username", username).getResultList();
		 StringBuilder sb = new StringBuilder();
		 for (Job job : jobs) {
		 sb.append(job.getId() + ", " + job.getStatus() + "\n");
		 }
		 return sb.toString();
	}

	public String getStatus(String jobId, String sessionId) throws SessionException,
			ForbiddenException {
		Job job = getJob(sessionId, jobId);
		StringBuilder sb = new StringBuilder();
		sb.append("Id:                 " + job.getId() + "\n");
		sb.append("Status:             " + job.getStatus() + "\n");
		sb.append("Comment:            " + job.getComment() + "\n");
		sb.append("Date of submission: " + job.getSubmitDate() + "\n");
		sb.append("Node:               " + job.getWorkerNode() + "\n");
		return sb.toString();
	}

	private Job getJob(String sessionId, String jobId) throws SessionException, ForbiddenException {
		checkCredentials(sessionId);
		String username = getUserName(sessionId);
		Job job = entityManager.find(Job.class, jobId);
		if (job == null || !job.getUsername().equals(username)) {
			throw new ForbiddenException("Job does not belong to you");
		}
		return job;
	}

	public void delete(String sessionId, String jobId) throws SessionException, ForbiddenException,
			InternalException, ParameterException {
		checkCredentials(sessionId);
		Job job = getJob(sessionId, jobId);
		if (!job.getStatus().equals("C")) {
			throw new ParameterException(
					"Only completed jobs can be deleted - try cancelling first");
		}
		for (String oe : new String[] { "o", "e" }) {
			String ext = "." + oe + jobId.split("\\.")[0];
			Path path = FileSystems.getDefault().getPath("/home/batch/jobs",
					job.getBatchFilename() + ext);
			try {
				Files.deleteIfExists(path);
			} catch (IOException e) {
				throw new InternalException("Unable to delete " + path.toString());
			}
		}
		entityManager.remove(job);
	}

	public void cancel(String sessionId, String jobId) throws SessionException, ForbiddenException,
			InternalException {
		checkCredentials(sessionId);
		Job job = getJob(sessionId, jobId);
		ShellCommand sc = new ShellCommand("qdel", job.getId());
		if (sc.isError()) {
			throw new InternalException("Unable to cancel job " + sc.getStderr());
		}

	}

	private void checkCredentials(String sessionId) {
		if (sessionId == null) {
			throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
					.entity("No sessionId was specified\n").build());
		}
	}

	public String submit(String sessionId, String executable, List<String> parameters,
			String family, boolean interactive) throws InternalException, SessionException,
			ParameterException {
		if (interactive) {
			return submitInteractive(sessionId, executable, parameters, family);
		} else {
			return submitBatch(sessionId, executable, parameters, family);
		}
	}

}
