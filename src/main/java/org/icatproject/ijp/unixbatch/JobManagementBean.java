package org.icatproject.ijp.unixbatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.namespace.QName;

import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.IcatExceptionType;
import org.icatproject.ijp.batch.BatchJson;
import org.icatproject.ijp.batch.JobStatus;
import org.icatproject.ijp.batch.OutputType;
import org.icatproject.ijp.batch.exceptions.ForbiddenException;
import org.icatproject.ijp.batch.exceptions.InternalException;
import org.icatproject.ijp.batch.exceptions.ParameterException;
import org.icatproject.ijp.batch.exceptions.SessionException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session Bean implementation to manage job status
 */
@Stateless
public class JobManagementBean {

	private String defaultFamily;
	private Map<String, List<String>> families = new HashMap<>();

	private Path jobOutputDir;

	private QName qName = new QName("http://icatproject.org", "ICATService");

	@PostConstruct
	void init() {

		try {
			CheckedProperties props = new CheckedProperties();
			props.loadFromFile(Constants.PROPERTIES_FILEPATH);

			String familiesList = props.getString("families.list");
			for (String mnemonic : familiesList.split("\\s+")) {
				if (defaultFamily == null) {
					defaultFamily = mnemonic;
				}
				String key = "families." + mnemonic;
				String[] members = props.getString(key).split("\\s+");
				families.put(mnemonic, new ArrayList<>(Arrays.asList(members)));
				logger.debug("Family " + mnemonic + " contains " + families.get(mnemonic));
			}
			if (defaultFamily == null) {
				String msg = "No families defined";
				logger.error(msg);
				throw new IllegalStateException(msg);
			}

			jobOutputDir = props.getPath("jobOutputDir");
			if (!jobOutputDir.toFile().exists()) {
				String msg = "jobOutputDir " + jobOutputDir + "does not exist";
				logger.error(msg);
				throw new IllegalStateException(msg);
			}
			jobOutputDir = jobOutputDir.toAbsolutePath();

			logger.info("Set up unixbatch with default family " + defaultFamily);
		} catch (Exception e) {
			String msg = e.getClass() + " reports " + e.getMessage();
			logger.error(msg);
			throw new IllegalStateException(msg);
		}

	}

	private final static Logger logger = LoggerFactory.getLogger(JobManagementBean.class);
	private final static Random random = new Random();

	@PersistenceContext(unitName = "unixbatch")
	private EntityManager entityManager;

	public InputStream getJobOutput(String jobId, OutputType outputType, String sessionId, String icatUrl)
			throws ForbiddenException, InternalException, ParameterException, SessionException {
		logger.info("getJobOutput called with sessionId:" + sessionId + " jobId:" + jobId + " outputType:" + outputType);
		UnixBatchJob job = getJob(jobId, sessionId, icatUrl);

		Path file = jobOutputDir.resolve(job.getDirectory()).resolve(
				outputType == OutputType.STANDARD_OUTPUT ? "o" : "e");

		if (Files.exists(file)) {
			try {
				return Files.newInputStream(file);
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} else {
			throw new ParameterException("No output file of type " + outputType + " available at the moment");
		}
	}

	public String submitBatch(String userName, String executable, List<String> parameters, String family)
			throws ParameterException, InternalException {

		if (family == null) {
			family = defaultFamily;
		}
		List<String> members = families.get(family);
		if (members == null) {
			throw new ParameterException("Specified family " + family + " is not recognised");
		}
		String owner = members.get(random.nextInt(members.size()));

		Path dir = null;
		try {
			dir = Files.createTempDirectory(jobOutputDir, null);
			ShellCommand sc = new ShellCommand("setfacl", "-m", "user:" + owner + ":rwx", dir.toString());
			if (sc.getExitValue() != 0) {
				throw new InternalException(sc.getMessage() + ". Check that user '" + owner + "' exists");
			}
		} catch (IOException e) {
			throw new InternalException("Unable to submit job via batch " + e.getClass() + " " + e.getMessage());
		}

		Path batchScriptFile = createScript(parameters, executable, dir);

		logger.debug("Writing to " + batchScriptFile.toString() + " to run as " + owner);
		String jobId;
		try (InputStream is = Files.newInputStream(batchScriptFile)) {
			ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), is, "sudo", "-u", owner, "batch");
			if (sc.getExitValue() != 0) {
				throw new InternalException("Unable to submit job via batch " + sc.getMessage() + sc.getStdout());
			}
			String response = sc.getStderr();
			
			// Some versions of the batch command send extra warnings to stderr;
			// we can't assume that the job id is always the second word.
			
			Pattern p = Pattern.compile(".*job (\\d+) .*", Pattern.DOTALL);
			Matcher m = p.matcher(response);
			if (m.matches()) {
				jobId = m.group(1);
			} else {
				throw new InternalException("Unable to extract job id from batch output: " + response);
			}

			UnixBatchJob job = new UnixBatchJob();
			job.setId(jobId);
			job.setExecutable(executable);
			job.setBatchUsername(owner);
			job.setUsername(userName);
			job.setSubmitDate(new Date());
			job.setDirectory(dir.getFileName().toString());
			entityManager.persist(job);
			logger.debug("Job " + jobId + " submitted");
			return jobId;
		} catch (IOException e) {
			throw new InternalException("Unable to submit job via batch " + e.getClass() + " " + e.getMessage());
		}

	}

	private Path createScript(List<String> parameters, String executable, Path dir) throws InternalException {
		Path batchScriptFile = null;
		try {
			batchScriptFile = Files.createTempFile(null, null);
		} catch (IOException e) {
			throw new InternalException("Unable to create a temporary file: " + e.getMessage());
		}

		String of = dir.resolve("o").toString();
		String ef = dir.resolve("e").toString();
		try (BufferedWriter bw = Files.newBufferedWriter(batchScriptFile, Charset.defaultCharset())) {
			writeln(bw, "#!/bin/sh");
			writeln(bw, "rm -rf *");
			writeln(bw, "echo $(date) - " + executable + " starting > " + of + " 2> " + ef);
			String line = executable + " " + JobManagementBean.escaped(parameters) + " >> " + of + " 2>> " + ef;
			writeln(bw, line);
			writeln(bw, "rc=$?");
			writeln(bw, "echo $(date) - " + executable + " ending with code $rc >> " + of + " 2>> " + ef);
			writeln(bw, "rm -rf *");
		} catch (IOException e) {
			throw new InternalException("Exception creating batch script: " + e.getMessage());
		}
		return batchScriptFile;
	}

	private void writeln(BufferedWriter bw, String string) throws IOException {
		bw.write(string);
		bw.newLine();
		logger.debug("Script line: " + string);
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

	private String getUserName(String sessionId, String icatUrl) throws ParameterException, SessionException {
		try {
			checkCredentials(sessionId, icatUrl);
			ICATService service = new ICATService(new URL(new URL(icatUrl), "ICATService/ICAT?wsdl"), qName);
			return service.getICATPort().getUserName(sessionId);
		} catch (IcatException_Exception e) {
			if (e.getFaultInfo().getType() == IcatExceptionType.SESSION) {
				throw new SessionException("IcatException " + e.getFaultInfo().getType() + " " + e.getMessage());
			} else {
				throw new ParameterException("IcatException " + e.getFaultInfo().getType() + " " + e.getMessage());
			}
		} catch (MalformedURLException e) {
			throw new ParameterException("Bad URL " + e.getMessage());
		}
	}

	public String list(String sessionId, String icatUrl) throws ParameterException, InternalException, SessionException {
		logger.info("listStatus called with sessionId:" + sessionId);

		String username = getUserName(sessionId, icatUrl);
		List<String> jobs = entityManager.createNamedQuery(UnixBatchJob.ID_BY_USERNAME, String.class)
				.setParameter("username", username).getResultList();
		return BatchJson.list(jobs);
	}

	public String getStatus(String jobId, String sessionId, String icatUrl) throws ForbiddenException,
			ParameterException, InternalException, SessionException {
		logger.info("getStatus called with sessionId:" + sessionId + " jobId:" + jobId);
		UnixBatchJob job = getJob(jobId, sessionId, icatUrl);
		if (job.isCancelled()) {
			logger.debug("job " + jobId + " has been cancelled");
			return BatchJson.getStatus(JobStatus.Cancelled);
		}
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being run by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner, "atq");
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}

		JobStatus status = JobStatus.Completed;
		for (String atq : sc.getStdout().trim().split("[\\n\\r]+")) {
			if (!atq.isEmpty()) {
				String[] bits = atq.split("\\s+");
				if (bits[0].equals(jobId)) {
					if (bits[3].equals("=")) {
						status = JobStatus.Executing;
					} else {
						status = JobStatus.Queued;
					}
					break;
				}
			}
		}
		return BatchJson.getStatus(status);
	}

	private UnixBatchJob getJob(String jobId, String sessionId, String icatUrl) throws ForbiddenException,
			ParameterException, SessionException {
		String username = getUserName(sessionId, icatUrl);
		if (jobId == null) {
			throw new ParameterException("No jobId was specified");
		}
		UnixBatchJob job = entityManager.find(UnixBatchJob.class, jobId);
		if (job == null || !job.getUsername().equals(username)) {
			throw new ForbiddenException("Job does not belong to you");
		}
		return job;
	}

	public void delete(String jobId, String sessionId, String icatUrl) throws ForbiddenException, InternalException,
			ParameterException, SessionException {
		logger.info("delete called with sessionId:" + sessionId + " jobId:" + jobId);
		UnixBatchJob job = getJob(jobId, sessionId, icatUrl);
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being run by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner, "atq");
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		String status = "Completed";
		for (String atq : sc.getStdout().trim().split("[\\n\\r]+")) {
			if (!atq.isEmpty()) {
				String[] bits = atq.split("\\s+");
				if (bits[0].equals(jobId)) {
					if (bits[3].equals("=")) {
						status = "Executing";
					} else {
						status = "Queued";
					}
					break;
				}
			}
		}
		logger.debug("Status is " + status);
		if (!status.equals("Completed")) {
			throw new ParameterException("Job " + jobId + " is " + status);
		}

		entityManager.remove(job);

		try {
			Path dir = jobOutputDir.resolve(job.getDirectory());
			File[] files = dir.toFile().listFiles();
			if (files != null) {
				for (File f : dir.toFile().listFiles()) {
					Files.delete(f.toPath());
				}
				Files.delete(dir);
				logger.debug("Directory " + dir + " has been deleted");
			}
		} catch (IOException e) {
			throw new InternalException("Unable to delete jobOutputDirectory " + job.getDirectory());
		}
	}

	public void cancel(String jobId, String sessionId, String icatUrl) throws ParameterException, ForbiddenException, SessionException {
		logger.info("cancel called with sessionId:" + sessionId + " jobId:" + jobId);
		UnixBatchJob job = getJob(jobId, sessionId, icatUrl);
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being handled by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner, "atrm", jobId);
		if (sc.isError()) {
			if (sc.getStderr().startsWith("Warning")) { // Job was running
				killJobsFor(owner);
			} else {
				throw new ParameterException(sc.getStderr());
			}
		}
		job.setCancelled(true);
	}

	private void killJobsFor(String owner) {
		ShellCommand sc = new ShellCommand("ps", "-U", owner, "-o", "pid=");
		if (!sc.isError()) {
			List<String> cmdbits = new ArrayList<>();
			cmdbits.add("sudo");
			cmdbits.add("-u");
			cmdbits.add(owner);
			cmdbits.add("/usr/bin/kill");
			cmdbits.add("-9");
			for (String pid : sc.getStdout().split("[\\r\\n]+")) {
				cmdbits.add(pid);
			}
			logger.debug("Executing " + cmdbits);
			sc = new ShellCommand(cmdbits);
			if (sc.isError()) {
				logger.debug(sc.getStderr());
			}
		}

	}

	private void checkCredentials(String sessionId, String icatUrl) throws ParameterException {
		if (sessionId == null) {
			throw new ParameterException("No sessionId was specified");
		}
		if (icatUrl == null) {
			throw new ParameterException("No icatUrl was specified");
		}
	}

	public String submit(String executable, List<String> parameters, String family, boolean interactive,
			String sessionId, String icatUrl) throws InternalException, ParameterException, SessionException {
		logger.info("submit called with sessionId:" + sessionId + " executable:" + executable + " parameters:"
				+ parameters + " family:" + family + " :" + " interactive:" + interactive);
		String userName = getUserName(sessionId, icatUrl);
		if (interactive) {
			throw new ParameterException("Interactive jobs are not currently supported by UnixBatch");
		} else {
			return BatchJson.submitBatch(submitBatch(userName, executable, parameters, family));
		}
	}

	public String estimate(String executable, List<String> parameters, String family, boolean interactive,
			String sessionId, String icatUrl) throws ParameterException, SessionException {
		logger.info("estimate called with sessionId:" + sessionId + " executable:" + executable + " parameters:"
				+ parameters + " family:" + family + " :" + " interactive:" + interactive);
		String userName = getUserName(sessionId, icatUrl);

		if (interactive) {
			return BatchJson.estimate(estimateInteractive(userName, executable, parameters, family));
		} else {
			return BatchJson.estimate(estimateBatch(userName, executable, parameters, family));
		}
	}

	private int estimateBatch(String userName, String executable, List<String> parameters, String family) {
		return 0;
	}

	private int estimateInteractive(String userName, String executable, List<String> parameters, String family)
			throws ParameterException {
		throw new ParameterException("Interactive jobs are not currently supported by UnixBatch");
	}

}
