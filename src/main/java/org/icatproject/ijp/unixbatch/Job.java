package org.icatproject.ijp.unixbatch;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQuery;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@SuppressWarnings("serial")
@Entity
@NamedQuery(name = "Job.FIND_BY_USERNAME", query = "SELECT j FROM Job j WHERE j.username = :username ORDER BY j.submitDate DESC")
public class Job implements Serializable {

	public final static String FIND_BY_USERNAME = "Job.FIND_BY_USERNAME";

	private String batchUsername;

	private String directory;
	private String executable;

	@Id
	private String id;

	@Temporal(TemporalType.TIMESTAMP)
	private Date submitDate;

	private String username;

	public Job() {
	}

	public String getBatchUsername() {
		return batchUsername;
	}

	public String getDirectory() {
		return directory;
	}

	public String getExecutable() {
		return executable;
	}

	public String getId() {
		return id;
	}

	public Date getSubmitDate() {
		return submitDate;
	}

	public String getUsername() {
		return username;
	}

	public void setBatchUsername(String batchUsername) {
		this.batchUsername = batchUsername;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public void setExecutable(String executable) {
		this.executable = executable;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setSubmitDate(Date submitDate) {
		this.submitDate = submitDate;
	}

	public void setUsername(String username) {
		this.username = username;
	}
}
