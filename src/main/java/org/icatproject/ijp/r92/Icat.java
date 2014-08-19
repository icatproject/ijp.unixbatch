package org.icatproject.ijp.r92;

import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;

/**
 * Provides an ICAT instance. If there is a problem an exception will be thrown. It will continue to
 * throw the same exception if called again.
 */
public class Icat {

	private static ICAT instance;

	synchronized public static ICAT getIcat() throws InternalException {
		if (instance == null) {
			try {
				CheckedProperties portalProps = new CheckedProperties();

				portalProps.loadFromFile(Constants.PROPERTIES_FILEPATH);

				if (portalProps.has("javax.net.ssl.trustStore")) {
					System.setProperty("javax.net.ssl.trustStore",
							portalProps.getProperty("javax.net.ssl.trustStore"));
				}
				URL icatUrl = portalProps.getURL("icat.url");

				icatUrl = new URL(icatUrl, "ICATService/ICAT?wsdl");

				QName qName = new QName("http://icatproject.org", "ICATService");
				ICATService service = new ICATService(icatUrl, qName);
				instance = service.getICATPort();
			} catch (MalformedURLException e) {
				throw new InternalException("Malformed URL :" + e.getMessage());
			} catch (CheckedPropertyException e) {
				throw new InternalException(e.getMessage());
			}
		}
		return instance;
	}

}
