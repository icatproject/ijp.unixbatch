package org.icatproject.ijp.r92;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.icatproject.ijp.r92.exceptions.InternalException;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class LoadFinder {

	class GangliaParser extends DefaultHandler {

		private Map<String, Float> loads = new HashMap<String, Float>();
		private String host;

		void reset() {
			host = null;
			loads.clear();
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			if (qName.equals("HOST")) {
				host = attributes.getValue("NAME");
			} else if (qName.equals("METRIC") && attributes.getValue("NAME").equals("load_one")) {
				loads.put(host, Float.parseFloat(attributes.getValue("VAL")));
			}
		}
	}

	private String gangliaHost;
	private GangliaParser gangliaParser;
	private XMLReader xmlReader;

	public LoadFinder() throws InternalException {

		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromFile(Constants.PROPERTIES_FILEPATH);
			gangliaHost = props.getString("gangliaHost");
		} catch (CheckedPropertyException e) {
			throw new InternalException("CheckedPropertyException " + e.getMessage());
		}

		try {
			new Socket(gangliaHost, 8649);
		} catch (UnknownHostException e) {
			throw new InternalException(gangliaHost + " host is not known");
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " reports " + e.getMessage());
		}

		gangliaParser = new GangliaParser();
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();

		try {
			xmlReader = saxFactory.newSAXParser().getXMLReader();
		} catch (SAXException e) {
			throw new InternalException("SAX Exception " + e.getMessage());
		} catch (ParserConfigurationException e) {
			throw new InternalException("SAX Parser Configuration Exception " + e.getMessage());
		}
		this.xmlReader.setContentHandler(gangliaParser);
		this.xmlReader.setErrorHandler(gangliaParser);

	}

	public Map<String, Float> getLoads() throws InternalException {
		Socket socket = null;
		try {
			socket = new Socket(gangliaHost, 8649);
			gangliaParser.reset();
			xmlReader.parse(new InputSource(socket.getInputStream()));
			return gangliaParser.loads;
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " reports " + e.getMessage());
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					// Do nothing
				}
			}
		}
	}

}
