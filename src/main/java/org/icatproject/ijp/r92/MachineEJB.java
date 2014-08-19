package org.icatproject.ijp.r92;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.rmi.ServerException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.ejb.Schedule;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceContext;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;

import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class MachineEJB {

	final static Logger logger = LoggerFactory.getLogger(MachineEJB.class);

	private long passwordDurationMillis;

	private String prepareaccount;

	private LoadFinder loadFinder;

	private ICAT icat;

	@PostConstruct
	private void init() {
		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromFile(Constants.PROPERTIES_FILEPATH);
			passwordDurationMillis = props.getPositiveInt("passwordDurationSeconds") * 1000L;
			poolPrefix = props.getString("poolPrefix");
			prepareaccount = props.getString("prepareaccount");
			logger.debug("Machine Manager Initialised");
		} catch (CheckedPropertyException e) {
			throw new RuntimeException("CheckedPropertyException " + e.getMessage());
		}
		try {
			pbs = new Pbs();
			loadFinder = new LoadFinder();
			pbs = new Pbs();
			icat = Icat.getIcat();
		} catch (InternalException e) {
			throw new RuntimeException("ServerException " + e.getMessage());
		}
	}

	@PersistenceContext(unitName = "r92")
	private EntityManager entityManager;

	private String poolPrefix;

	private Pbs pbs;

	private final static Random random = new Random();
	private final static String chars = "abcdefghijkmnpqrstuvwxyz23456789";

	private Account getAccount(String lightest, String sessionId, String jobName,
			List<String> parameters, File script) throws InternalException {
		String userName;
		try {
			userName = icat.getUserName(sessionId);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getMessage());
		}
		logger.debug("Set up account for " + userName + " on " + lightest);

		logger.debug("Need to create a new pool account");
		Account account = new Account();
		account.setHost(lightest);
		entityManager.persist(account);
		char[] pw = new char[4];
		for (int i = 0; i < pw.length; i++) {
			pw[i] = chars.charAt(random.nextInt(chars.length()));
		}
		String password = new String(pw);

		Long id = account.getId();

		ShellCommand sc = new ShellCommand("scp", script.getAbsolutePath(), "dmf@" + lightest + ":"
				+ id + ".sh");
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}

		List<String> args = Arrays.asList("ssh", lightest, prepareaccount, poolPrefix + id,
				password, id + ".sh");
		sc = new ShellCommand(args);
		if (sc.isError()) {
			throw new InternalException(sc.getMessage());
		}
		if (!sc.getStdout().isEmpty()) {
			logger.debug("Prepare account reports " + sc.getStdout());
		}
		account.setUserName(userName);
		account.setAllocatedDate(new Date());

		account.setPassword(password);

		return account;
	}

	@Schedule(minute = "*/1", hour = "*")
	private void cleanAccounts() {
		try {
			/* First find old accounts and remove their password */
			Date passwordTime = new Date(System.currentTimeMillis() - passwordDurationMillis);
			List<Account> accounts = entityManager.createNamedQuery(Account.OLD, Account.class)
					.setParameter("date", passwordTime).getResultList();
			for (Account account : accounts) {
				entityManager.refresh(account, LockModeType.PESSIMISTIC_WRITE);
				logger.debug("Delete password for account " + account.getId() + " on "
						+ account.getHost());
				ShellCommand sc = new ShellCommand("ssh", account.getHost(), "sudo",
						"/usr/bin/passwd", "-d", poolPrefix + account.getId());
				if (sc.isError()) {
					throw new RuntimeException(sc.getMessage());
				}
				logger.debug("Command passwd reports " + sc.getStdout());
				account.setAllocatedDate(null);
			}

			/* Now delete any accounts which have no processes running */
			accounts = entityManager.createNamedQuery(Account.TODELETE, Account.class)
					.getResultList();
			boolean deleted = false;
			for (Account account : accounts) {
				ShellCommand sc = new ShellCommand("ssh", account.getHost(), "ps", "-F",
						"--noheaders", "-U", poolPrefix + account.getId());
				if (sc.getExitValue() == 1
						&& sc.getStderr().startsWith("ERROR: User name does not exist")) {
					/* Account seems to have vanished */
					entityManager.remove(account);
					deleted = true;
					logger.warn("Account for " + poolPrefix + account.getId() + " on "
							+ account.getHost() + " has vanished!");
				} else if (!sc.getStderr().isEmpty()) {
					/* Odd condition because no processes has error code 1 */
					logger.error("Unexpected problem using ssh to connect to " + account.getHost()
							+ " to find proceeses for " + poolPrefix + account.getId());
					throw new RuntimeException(sc.getMessage());
				} else if (sc.getStdout().isEmpty()) {
					logger.debug("No processes running for " + poolPrefix + account.getId());
					sc = new ShellCommand("ssh", account.getHost(), "sudo", "userdel", "-r",
							poolPrefix + account.getId());
					if (sc.isError()) {
						throw new RuntimeException(sc.getMessage());
					}
					logger.debug("Command userdel for " + poolPrefix + account.getId() + " on "
							+ account.getHost() + " reports " + sc.getStdout());
					entityManager.remove(account);
					deleted = true;
				} else {
					logger.debug(poolPrefix + account.getId() + " has "
							+ sc.getStdout().split("\\n").length + " processes running on "
							+ account.getHost());
				}
			}

			/*
			 * If an account was deleted consider putting machines back on line. This checks all
			 * machines not only the one for which the account was deleted to help recover if the
			 * system gets into a strange state.
			 */
			if (deleted) {
				Map<String, String> avail = pbs.getStates();
				for (Entry<String, String> pair : avail.entrySet()) {
					boolean online = true;
					for (String state : pair.getValue().split(",")) {
						if (state.equals("offline")) {
							online = false;
							break;
						}
					}
					if (!online) {
						String hostName = pair.getKey();
						long count = entityManager.createNamedQuery(Account.USERS, Long.class)
								.setParameter("host", hostName).getSingleResult();
						if (count == 0L) {
							logger.debug("Idle machine " + hostName + " has no users");
							pbs.setOnline(hostName);
						} else {
							logger.debug("Idle machine " + hostName + " has " + count
									+ " users so cannot be put back online");
						}
					}
				}
			}

		} catch (Throwable e) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			e.printStackTrace(ps);
			logger.error("cleanAccounts failed " + baos.toString());
		}
	}

	public Account prepareMachine(String sessionId, String jobName, List<String> parameters,
			File script) throws InternalException {
		Set<String> machines = new HashSet<String>();
		Map<String, Float> loads = loadFinder.getLoads();
		Map<String, String> avail = pbs.getStates();
		for (Entry<String, String> pair : avail.entrySet()) {
			boolean online = true;
			for (String state : pair.getValue().split(",")) {
				if (state.equals("offline")) {
					online = false;
					break;
				}
			}
			if (online) {
				logger.debug(pair.getKey() + " is currently online");
				machines.add(pair.getKey());
			}
		}
		if (machines.isEmpty()) {
			machines = avail.keySet();
			if (machines.isEmpty()) {
				throw new InternalException("No machines available");
			}
		}

		String lightest = null;
		for (String machine : machines) {
			if (lightest == null || loads.get(machine) < loads.get(lightest)) {
				lightest = machine;
			}
		}

		pbs.setOffline(lightest);
		return getAccount(lightest, sessionId, jobName, parameters, script);

	}

	public String getPoolPrefix() {
		return poolPrefix;
	}
}
