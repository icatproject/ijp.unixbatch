package org.icatproject.ijp.r92;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@SuppressWarnings("serial")
@Entity
@NamedQueries({
		@NamedQuery(name = "Job.FIND_BY_USERNAME", query = "SELECT j FROM Job j WHERE j.username = :username ORDER BY j.submitDate DESC"),
		@NamedQuery(name = "Job.FIND_INCOMPLETE", query = "SELECT j FROM Job j WHERE j.status != 'C'") })
public class Job implements Serializable {

	public final static String FIND_BY_USERNAME = "Job.FIND_BY_USERNAME";
	public final static String FIND_INCOMPLETE = "Job.FIND_INCOMPLETE";

	@Id
	private String id;

	private String status;

	private String username;

	@Temporal(TemporalType.TIMESTAMP)
	private Date submitDate;

	private long icatJobId;

	private String batchFilename;

	private String workerNode;

	private String comment;

	public String getBatchUsername() {
		return batchUsername;
	}

	private String batchUsername;

	public String getComment() {
		return comment;
	}

	public Job() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getSubmitDate() {
		return submitDate;
	}

	public void setSubmitDate(Date submitDate) {
		this.submitDate = submitDate;
	}

	public long getIcatJobId() {
		return icatJobId;
	}

	public void setIcatJobId(long icatJobId) {
		this.icatJobId = icatJobId;
	}

	public String getBatchFilename() {
		return batchFilename;
	}

	public void setBatchFilename(String batchFilename) {
		this.batchFilename = batchFilename;
	}

	public String getWorkerNode() {
		return workerNode;
	}

	public void setWorkerNode(String workerNode) {
		this.workerNode = workerNode;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public void setBatchUsername(String batchUsername) {
		this.batchUsername = batchUsername;
	}
}
