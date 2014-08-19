package org.icatproject.ijp.r92;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Pbs {

	final static Logger logger = LoggerFactory.getLogger(Pbs.class);

	class PbsParser extends DefaultHandler {

		private Map<String, String> states = new HashMap<String, String>();
		private Map<String, String> jobs = new HashMap<String, String>();
		private String host;
		private String state;
		private String job;
		private StringBuilder text = new StringBuilder();

		void reset() {
			host = null;
			state = null;
			job = null;
			text.setLength(0);
			states.clear();
			jobs.clear();
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			text.setLength(0);
			if (qName.equals("Node")) {
				host = null;
				state = null;
				job = null;
			}

		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (qName.equals("Node") && host != null) {
				if (state != null) {
					states.put(host, state);
				}
				if (job != null) {
					jobs.put(host, job);
				}
			} else if (qName.equals("name")) {
				host = text.toString();
			} else if (qName.equals("state")) {
				state = text.toString();
			} else if (qName.equals("jobs")) {
				job = text.toString();
			}
		}

		@Override
		public void characters(char ch[], int start, int length) throws SAXException {
			text.append(ch, start, length);
		}
	}

	private PbsParser pbsParser;
	private XMLReader xmlReader;
	private String pbsnodes;
	private String qsig;
	private String qstat;
	private String qsub;
	private Unmarshaller qstatUnmarshaller;

	public Pbs() throws InternalException {

		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromFile(Constants.PROPERTIES_FILEPATH);
			pbsnodes = props.getString("pbsnodes");
			qsig = props.getString("qsig");
			qstat = props.getString("qstat");
			qsub = props.getString("qsub");
		} catch (CheckedPropertyException e) {
			throw new InternalException("CheckedPropertyException " + e.getMessage());
		}

		pbsParser = new PbsParser();
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();

		try {
			xmlReader = saxFactory.newSAXParser().getXMLReader();
		} catch (SAXException e) {
			throw new InternalException("SAX Exception " + e.getMessage());
		} catch (ParserConfigurationException e) {
			throw new InternalException("SAX Parser Configuration Exception " + e.getMessage());
		}
		this.xmlReader.setContentHandler(pbsParser);
		this.xmlReader.setErrorHandler(pbsParser);

		try {
			qstatUnmarshaller = JAXBContext.newInstance(Qstat.class).createUnmarshaller();
		} catch (JAXBException e) {
			throw new InternalException("Unable to create marshaller " + e.getMessage());
		}

	}

	public Map<String, String> getStates() throws InternalException {
		parse();
		/*
		 * A copy of the map is returned so that subsequent calls to setOffline or setOnline - both
		 * of which call parse - don't mess up the map.
		 */
		return new HashMap<String, String>(pbsParser.states);
	}

	private void parse() throws InternalException {
		ShellCommand sc = new ShellCommand(pbsnodes, "-x");
		if (sc.isError()) {
			throw new InternalException("Code " + sc.getExitValue() + ": " + sc.getStderr());
		}
		pbsParser.reset();
		try {
			xmlReader.parse(new InputSource(new StringReader(sc.getStdout())));
		} catch (Exception e) {
			throw new InternalException(e.getMessage());
		}
	}

	private Map<String, String> getJobs() throws InternalException {
		parse();
		return pbsParser.jobs;
	}

	public void setOffline(String hostName) throws InternalException {
		ShellCommand sc = new ShellCommand(pbsnodes, "-o", hostName);
		if (sc.isError()) {
			throw new InternalException("Code " + sc.getExitValue() + ": " + sc.getStderr());
		}
		String jobs = getJobs().get(hostName);
		if (jobs != null) {
			for (String job : jobs.split(",")) {
				suspendJob(job.split("/")[1]);
			}
		}
		logger.debug(hostName + " is now offline");
	}

	public void setOnline(String hostName) throws InternalException {
		ShellCommand sc = new ShellCommand(pbsnodes, "-c", hostName);
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		sc = new ShellCommand(qstat, "-x");
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		String jobsXml = sc.getStdout().trim();
		if (jobsXml.isEmpty()) {
			return;
		}
		Qstat qstat;
		try {
			qstat = (Qstat) qstatUnmarshaller.unmarshal(new StringReader(jobsXml));
		} catch (JAXBException e) {
			throw new InternalException("Unable to unmarshall qstat output " + e.getMessage() + " "
					+ jobsXml);
		}
		for (Qstat.Job xjob : qstat.getJobs()) {
			String id = xjob.getJobId();
			String status = xjob.getStatus();
			String wn = xjob.getWorkerNode();
			String workerNode = wn != null ? wn.split("/")[0] : "";
			if ("S".equals(status) && hostName.equals(workerNode)) {
				resumeJob(id);
			}
		}
		sc = new ShellCommand(qsub, "-o", "/dev/null", "-e", "/dev/null", "/home/dmf/bin/wakeup");
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		if (sc.getStdout() != null) {
			logger.debug("qsub reports " + sc.getStdout());
		}
		logger.debug(hostName + " is now online");
	}

	private void resumeJob(String jobName) throws InternalException {
		ShellCommand sc = new ShellCommand(qsig, "-s", "resume", jobName);
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		logger.debug(jobName + " has now resumed");
	}

	private void suspendJob(String jobName) throws InternalException {
		ShellCommand sc = new ShellCommand(qsig, "-s", "suspend", jobName);
		if (sc.getExitValue() == 170) { // Invalid state for job
			logger.debug(sc.getMessage());
			return;
		}
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		logger.debug(jobName + " is now suspended");
	}

}
