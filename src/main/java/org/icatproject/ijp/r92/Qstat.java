package org.icatproject.ijp.r92;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Data")
public class Qstat {

	@XmlElement(required = true, name = "Job")
	protected List<Qstat.Job> jobs;

	public List<Qstat.Job> getJobs() {
		if (jobs == null) {
			jobs = new ArrayList<Qstat.Job>();
		}
		return this.jobs;
	}

	public static class Job {
		@XmlElement(required = true, name = "Job_Id")
		protected String jobId;

		@XmlElement(required = true, name = "exec_host")
		protected String workerNode;

		@XmlElement(required = true, name = "job_state")
		protected String status;

		@XmlElement(required = true, name = "Job_Name")
		protected String batchFilename;

		@XmlElement(required = true)
		private String comment;

		public String getJobId() {
			return jobId;
		}

		public String getBatchFilename() {
			return batchFilename;
		}

		public String getStatus() {
			return status;
		}

		public String getWorkerNode() {
			return workerNode;
		}

		public String getComment() {
			return comment;
		}

	}

}
