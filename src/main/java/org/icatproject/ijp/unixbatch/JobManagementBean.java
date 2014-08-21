package org.icatproject.ijp.unixbatch;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.ijp.unixbatch.exceptions.ForbiddenException;
import org.icatproject.ijp.unixbatch.exceptions.InternalException;
import org.icatproject.ijp.unixbatch.exceptions.ParameterException;
import org.icatproject.ijp.unixbatch.exceptions.SessionException;
import org.icatproject.utils.CheckedProperties;
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

	private String defaultFamily;
	private Map<String, List<String>> families = new HashMap<>();

	private Path jobOutputDir;

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
				String[] members = props.getProperty(key).split("\\s+");
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

			if (props.has("javax.net.ssl.trustStore")) {
				System.setProperty("javax.net.ssl.trustStore",
						props.getProperty("javax.net.ssl.trustStore"));
			}
			URL icatUrl = props.getURL("icat.url");
			icatUrl = new URL(icatUrl, "ICATService/ICAT?wsdl");
			QName qName = new QName("http://icatproject.org", "ICATService");
			ICATService service = new ICATService(icatUrl, qName);
			icat = service.getICATPort();

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

	public InputStream getJobOutput(String sessionId, String jobId, OutputType outputType)
			throws SessionException, ForbiddenException, InternalException, ParameterException {
		logger.info("getJobOutput called with sessionId:" + sessionId + " jobId:" + jobId
				+ " outputType:" + outputType);
		Job job = getJob(sessionId, jobId);

		Path file = jobOutputDir.resolve(job.getDirectory()).resolve(
				outputType == OutputType.STANDARD_OUTPUT ? "o" : "e");

		if (Files.exists(file)) {
			try {
				return Files.newInputStream(file);
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} else {
			throw new ParameterException("No output file of type " + outputType
					+ " available at the moment");
		}
	}

	public String submitBatch(String userName, String executable, List<String> parameters,
			String family) throws ParameterException, InternalException, SessionException {

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
			ShellCommand sc = new ShellCommand("setfacl", "-m", "user:" + owner + ":rwx",
					dir.toString());
			if (sc.getExitValue() != 0) {
				throw new InternalException(sc.getMessage() + ". Check that user '" + owner
						+ "' exists");
			}
		} catch (IOException e) {
			throw new InternalException("Unable to submit job via batch " + e.getClass() + " "
					+ e.getMessage());
		}

		Path batchScriptFile = createScript(parameters, executable, dir);

		logger.debug("Writing to " + batchScriptFile.toString() + " to run as " + owner);
		String jobId;
		try (InputStream is = Files.newInputStream(batchScriptFile)) {
			ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), is, "sudo", "-u",
					owner, "batch");
			if (sc.getExitValue() != 0) {
				throw new InternalException("Unable to submit job via batch " + sc.getMessage()
						+ sc.getStdout());
			}
			String response = sc.getStderr().trim();
			jobId = response.split("\\s+")[1];

			Job job = new Job();
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
			throw new InternalException("Unable to submit job via batch " + e.getClass() + " "
					+ e.getMessage());
		}

	}

	private Path createScript(List<String> parameters, String executable, Path dir)
			throws InternalException {
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
			String line = executable + " " + JobManagementBean.escaped(parameters) + " >> " + of
					+ " 2>> " + ef;
			writeln(bw, line);
			writeln(bw, "rc=$?");
			writeln(bw, "echo $(date) - " + executable + " ending with code $rc >> " + of + " 2>> "
					+ ef);
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

	public String submitInteractive(String userName, String executable, List<String> parameters,
			String family) throws InternalException {
		return null;
		// TODO must improve this ...
		// Path interactiveScriptFile = createScript(parameters, executable, null);
		// Account account = machineEJB.prepareMachine(userName, executable, parameters,
		// interactiveScriptFile);
		// return account.getUserName() + " " + account.getPassword() + " " + account.getHost();
	}

	private String getUserName(String sessionId) throws SessionException, ParameterException {
		try {
			checkCredentials(sessionId);
			return icat.getUserName(sessionId);
		} catch (IcatException_Exception e) {
			throw new SessionException("IcatException " + e.getFaultInfo().getType() + " "
					+ e.getMessage());
		}
	}

	public String listStatus(String sessionId) throws SessionException, ParameterException,
			InternalException {
		logger.info("listStatus called with sessionId:" + sessionId);

		String username = getUserName(sessionId);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos).writeStartArray();
		Map<String, Map<String, String>> jobs = new HashMap<>();
		for (Job job : entityManager.createNamedQuery(Job.FIND_BY_USERNAME, Job.class)
				.setParameter("username", username).getResultList()) {

			String jobId = job.getId();
			String owner = job.getBatchUsername();
			Map<String, String> jobsByOwner = jobs.get(owner);
			if (jobsByOwner == null) {
				jobsByOwner = new HashMap<>();
				jobs.put(owner, jobsByOwner);
				ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u",
						owner, "atq");
				if (sc.isError()) {
					throw new InternalException(sc.getMessage());
				}
				String status;
				for (String atq : sc.getStdout().trim().split("[\\n\\r]+")) {
					if (!atq.isEmpty()) {
						String[] bits = atq.split("\\s+");
						if (bits[3].equals("=")) {
							status = "Executing";
						} else {
							status = "Queued";
						}
						jobsByOwner.put(bits[0], status);
					}
				}
				logger.debug("Built list of jobs for " + owner + ": " + jobsByOwner);
			}
			String status = jobsByOwner.get(jobId);
			if (status == null) {
				status = "Completed";
			}
			gen.writeStartObject().write("Id", job.getId()).write("Status", status)
					.write("Executable", job.getExecutable())
					.write("Date of submission", job.getSubmitDate().toString()).writeEnd();
		}
		gen.writeEnd().close();
		return baos.toString();
	}

	public String getStatus(String jobId, String sessionId) throws SessionException,
			ForbiddenException, ParameterException, InternalException {
		logger.info("getStatus called with sessionId:" + sessionId + " jobId:" + jobId);
		Job job = getJob(sessionId, jobId);
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being run by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner,
				"atq");
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
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos);
		gen.writeStartObject().write("Id", jobId).write("Status", status)
				.write("Executable", job.getExecutable())
				.write("Date of submission", job.getSubmitDate().toString());
		gen.writeEnd().close();
		return baos.toString();
	}

	private Job getJob(String sessionId, String jobId) throws SessionException, ForbiddenException,
			ParameterException {
		String username = getUserName(sessionId);
		if (jobId == null) {
			throw new ParameterException("No jobId was specified");
		}
		Job job = entityManager.find(Job.class, jobId);
		if (job == null || !job.getUsername().equals(username)) {
			throw new ForbiddenException("Job does not belong to you");
		}
		return job;
	}

	public void delete(String sessionId, String jobId) throws SessionException, ForbiddenException,
			InternalException, ParameterException {
		logger.info("delete called with sessionId:" + sessionId + " jobId:" + jobId);
		Job job = getJob(sessionId, jobId);
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being run by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner,
				"atq");
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
			for (File f : dir.toFile().listFiles()) {
				Files.delete(f.toPath());
			}
			Files.delete(dir);
			logger.debug("Directory " + dir + " has been deleted");
		} catch (IOException e) {
			throw new InternalException("Unable to delete jobOutputDirectory " + job.getDirectory());
		}
	}

	public void cancel(String sessionId, String jobId) throws SessionException, ForbiddenException,
			InternalException, ParameterException {
		logger.info("cancel called with sessionId:" + sessionId + " jobId:" + jobId);
		Job job = getJob(sessionId, jobId);
		String owner = job.getBatchUsername();
		logger.debug("job " + jobId + " is being run by " + owner);
		ShellCommand sc = new ShellCommand(Paths.get("/home/" + owner), null, "sudo", "-u", owner,
				"atrm", jobId);
		if (sc.isError() && !sc.getStderr().startsWith("Warning")) {
			throw new ParameterException(sc.getStderr());
		}
	}

	private void checkCredentials(String sessionId) throws ParameterException {
		if (sessionId == null) {
			throw new ParameterException("No sessionId was specified");
		}
	}

	public String submit(String sessionId, String executable, List<String> parameters,
			String family, boolean interactive) throws InternalException, SessionException,
			ParameterException {
		logger.info("submit called with sessionId:" + sessionId + " executable:" + executable
				+ " parameters:" + parameters + " family:" + family + " :" + " interactive:"
				+ interactive);
		String userName = getUserName(sessionId);
		if (interactive) {
			return submitInteractive(userName, executable, parameters, family);
		} else {
			return submitBatch(userName, executable, parameters, family);
		}
	}

}
