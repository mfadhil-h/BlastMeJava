package com.blastme.messaging.toolpooler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.generators.BCrypt;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;

public class Tool {
	private Logger logger;
	private static SimpleDateFormat sdfStartEndHour;

	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");
	
	public Tool() {
		logger = LogManager.getLogger("TOOL");
		
		sdfStartEndHour = new SimpleDateFormat("yyMMddHH");
	}

	public String makeMD5(String str){
		return DigestUtils.md5Hex(str);
	}
	
	public Boolean makeBool(String str){
		if (str.equals("true")){
			return true;
		}else if(str.equals("1")){
			return true;
		}else if(str.equals("false")){
			return false;
		}else if(str.equals("0")){
			return false;
		}else{
			return false;
		}
	}
	
	public HashMap<String, String> convertStringToHashMap(String parIn){
		HashMap<String, String> hasil = new HashMap<String, String>();
		
		String value = parIn.substring(1, parIn.length()-1);
		String[] keyValuePairs = value.split(",");
		
		for(String pair : keyValuePairs){
			String[] entry = pair.split("=");
			hasil.put(entry[0].trim(), entry[1].trim());
		}
		
		return hasil;
	}
	
	public HashMap<String, Object> convertXMLToHashMap(String xmlParIn){
		HashMap<String, Object> hasil = new HashMap<String, Object>();
		
		// Convert to JSON
		try{
			JSONObject jsonObj = XML.toJSONObject(xmlParIn);
			
			Iterator<?> keys = jsonObj.keys();
			
			while(keys.hasNext()){
				String key = (String) keys.next();
				Object value = jsonObj.get(key);
				
				hasil.put(key, value);
			}
			
			hasil.put("convertingResultCode", "OK");
			hasil.put("convertingResultDesc", "Success to convert XML to HashMap.");
		} catch (Exception e){
			hasil.put("convertingResultCode", "FAILED");
			hasil.put("convertingResultDesc", "FAILED to convert XML to HashMap.");
			
			logger.debug("Converting XML to Hashmap is failed with error: " + e);
			logger.error("Converting XML to Hashmap is failed with error: " + e);
		}
		
		return hasil;
	}
	
	
	public HashMap<String, String> convertHashMapObjectToString(HashMap<String, Object> mapIn){
		HashMap<String, String> hasil = new HashMap<String, String>();
		
		for(Map.Entry<String, Object> entry : mapIn.entrySet()){
			try{
				hasil.put(entry.getKey(), (String) entry.getValue());
			} catch (ClassCastException e){
				logger.debug("Failed to convert Object HashMap to String HashMap. Error: " + e.toString());
				logger.error("Failed to convert Object HashMap to String HashMap. Error: " + e.toString());
			}
		}
		
		return hasil;
	}

	public HashMap<String, String> convertJSONToHashMap(JSONObject jsonIn){
		HashMap<String, String> mapHasil = new HashMap<String, String>();
		
		Iterator<String> keyIterator = jsonIn.keys();
		
		while(keyIterator.hasNext()){
			String key = keyIterator.next();
			String value = "";
			
			if(jsonIn.get(key) instanceof Long){
				value = Long.toString(jsonIn.getLong(key));
			} else if(jsonIn.get(key) instanceof Integer){
				value = Integer.toString(jsonIn.getInt(key));
			} else if(jsonIn.get(key) instanceof Double){
				value = Double.toString(jsonIn.getDouble(key));
			} else if(jsonIn.get(key) instanceof JSONObject){
				value = jsonIn.getJSONObject(key).toString();
			} else {
				value = jsonIn.getString(key);	
			}
						
			mapHasil.put(key, value);
		}
		
		return mapHasil;
	}
	
	public JSONObject convertHashMapToJSON(HashMap<String, String> mapIn){
		JSONObject jsonHasil = new JSONObject(); // for now it only supports simple string map, not complicated String.
		
		if(mapIn.size() > 0){
			for(Map.Entry<String, String> entry : mapIn.entrySet()){
				if((entry.getKey() != null) && (entry.getValue() != null)){
					jsonHasil.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		return jsonHasil;
	}
	
	public JSONObject convertHashMapJSONToJSON(HashMap<String, JSONObject> mapIn){
		JSONObject jsonHasil = new JSONObject();
		
		if(mapIn.size() > 0){
			for(Map.Entry<String, JSONObject> entry : mapIn.entrySet()){
				if((entry.getKey() != null) && (entry.getValue() != null)){
					jsonHasil.put(entry.getKey(), ((JSONObject) entry.getValue()).toString());
				}
			}
		}
		
		return jsonHasil;
	}
	
	public JSONObject convertHashMapIntegerToJSON(HashMap<String, Integer> mapIn){
		JSONObject jsonHasil = new JSONObject();
		
		if(mapIn.size() > 0){
			for(Map.Entry<String, Integer> entry : mapIn.entrySet()){
				if((entry.getKey() != null) && (entry.getValue() != null)){
					jsonHasil.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		return jsonHasil;
	}
	
	public String convertHashMapToXMLString(HashMap<String, String> mapIn, String root){
		String hasil = "";
		StringBuilder sb = new StringBuilder();
		if(root.length()>0){
			sb.append("<");
			sb.append(root);
			sb.append(">");
		}
		
		for(Map.Entry<String, String> e: mapIn.entrySet()){
			sb.append("<");
			sb.append(e.getKey());
			sb.append(">");
			
			sb.append(e.getValue());
			
			sb.append("</");
			sb.append(e.getKey());
			sb.append(">");
		}
		
		if(root.length()>0){
			sb.append("</");
			sb.append(root);
			sb.append(">");
		}
		
		hasil = sb.toString();
		
		return hasil;
	}
	
	public String generateUniqueID(){
		String hasil = "";
		
		UUID uniqueId = UUID.randomUUID();
		
		hasil = String.valueOf(uniqueId).replace("-", "");
		
		return hasil;
	}
	
	public String convertJSONObjectToXMLString(JSONObject jsonObj, String coverRoot){
		HashMap<String, String> tempMap = convertJSONToHashMap(jsonObj);
		String xmlHasil = convertHashMapToXMLString(tempMap, coverRoot);
		
		return xmlHasil;
	}
	
	public String convertJSONArrayToXMLString(JSONArray jsonArray, String coverRoot, String elementRoot){
		String hasil = "";
		
		hasil = hasil + "<" + coverRoot + ">";
		
//		String arrXML = XML.toString(jsonArray, elementRoot);
//		hasil = hasil + arrXML;
		
		
		// Loop through JSONArray
		for(int i = 0; i < jsonArray.length(); i++){
			// make the elementRoot
			hasil = hasil + "<" + elementRoot + ">";

			if(jsonArray.get(i) instanceof JSONObject){
				JSONObject jsonObj = jsonArray.getJSONObject(i);
				
				// iteratr through the JSONObject
				String[] elementNames = JSONObject.getNames(jsonObj);
				for(String elementName: elementNames){
					String elementValue = "";
					if(jsonObj.get(elementName) instanceof String){
						elementValue = jsonObj.getString(elementName);
					} else if(jsonObj.get(elementName) instanceof Integer){
						elementValue = Integer.toString(jsonObj.getInt(elementName));
					} else if(jsonObj.get(elementName) instanceof Double){
						elementValue = String.format("%2.f", jsonObj.getDouble(elementName));
					} else if(jsonObj.get(elementName) instanceof JSONArray){
						elementValue = convertJSONArrayToXMLString(jsonObj.getJSONArray(elementName), elementName, elementName);
					} else {
						elementValue = jsonObj.get(elementName).toString();
					}
					
					// Put it in array format
					if((elementValue != null) && (elementValue.length() > 0)){
						hasil = hasil + "<" + elementName + ">" + elementValue + "</" + elementName + ">"; 

					}
				}			
			} else {
				hasil = hasil + jsonArray.toString();
			}


			// close the elementRoot
			hasil = hasil + "</" + elementRoot + ">";

		}

		hasil = hasil + "</" + coverRoot + ">";
		
		logger.debug("Function convertJSONArrayToXMLString - hasil: " + hasil);
		
		return hasil;
	}
	
	public Boolean isValidJSONObject(String strJSONObjectPar){
		Boolean hasil = false;
		
		try{
			new JSONObject(strJSONObjectPar);
			
			hasil = true;
		} catch(Exception e){
			hasil = false;
		}
		
		return hasil;
	}
	
	public Boolean isValidJSONArray(String strJSONArrayPar){
		Boolean hasil = false;
		
		try{
			new JSONArray(strJSONArrayPar);
			
			hasil = true;
		} catch(Exception e){
			hasil = false;
		}
		
		return hasil;
	}
	
	public String convertJSONToXMLStringWithKeyAsXMLTag(JSONObject jsonParameter, Boolean TagUseLowerCase, Boolean withRootTag, String rootTag){
		String hasil = "";
		
		StringBuilder sb = new StringBuilder();
		
		if(TagUseLowerCase == true){
			rootTag = rootTag.toLowerCase();
			
			logger.debug("TagUserLowerCase is TRUE, rootTag: " + rootTag);
		}
		
		// Opening root tag
		if(withRootTag == true){
			if(rootTag.length()>0){
				sb.append("<");
				sb.append(rootTag);
				sb.append(">");
				
				logger.debug("withRootTag is TRUE, rootTag: " + rootTag);
			}
		}
		
		Iterator<?> itrJsonParameter = jsonParameter.keys();
		
		// Handle non JSONObject and non JSONArray data
		while(itrJsonParameter.hasNext()){
			// Original key
			String key = (String) itrJsonParameter.next();
			
			// Key changed to lowercase if TagUserLoweCase is true
			String finalKey = key;
			
			if(TagUseLowerCase == true){
				finalKey = key.toLowerCase();
			}
			
			// Put key's content
			String content = "";
			if(jsonParameter.get(key) instanceof String){
				String tempContent = jsonParameter.getString(key);
				
				if(isValidJSONObject(tempContent) == true){
					// Check apakah JSONObject dalam bentuk String
					content = convertJSONToXMLStringWithKeyAsXMLTag(jsonParameter.getJSONObject(key), TagUseLowerCase, false, "");
				} else if(isValidJSONArray(tempContent)  == true){
					// Check apakah JSONArray dalam bentuk String
					JSONArray jsonArPenampung = new JSONArray(tempContent);
					
					for(int x=0; x < jsonArPenampung.length(); x++){
						JSONObject jsonObjPenampung = jsonArPenampung.getJSONObject(x);
						
						content = content + convertJSONToXMLStringWithKeyAsXMLTag(jsonObjPenampung, TagUseLowerCase, false, "");
					}
				} else {
					// Just String
					content = tempContent;
				}
			} else if(jsonParameter.get(key) instanceof Integer){
				content = Integer.toString(jsonParameter.getInt(key));
			} else if(jsonParameter.get(key) instanceof Boolean){
				content = Boolean.toString(jsonParameter.getBoolean(key));
			} else if(jsonParameter.get(key) instanceof Long){
				content = Long.toString(jsonParameter.getLong(key));
			} else if(jsonParameter.get(key) instanceof Double){
				content = String.format("%.2f", jsonParameter.getDouble(key));
			} else if(jsonParameter.get(key) instanceof JSONObject){
				// If JSONObject, convert to XML that will become sub-XML without rootTag
				content = convertJSONToXMLStringWithKeyAsXMLTag(jsonParameter.getJSONObject(key), TagUseLowerCase, false, "");
			} else if(jsonParameter.get(key) instanceof JSONArray){
				// If JSONArray, loop through it, handle each JSONObject
				JSONArray jsonArPenampung = jsonParameter.getJSONArray(key);
				
				for(int x=0; x < jsonArPenampung.length(); x++){
					JSONObject jsonObjPenampung = jsonArPenampung.getJSONObject(x);
					
					content = content + convertJSONToXMLStringWithKeyAsXMLTag(jsonObjPenampung, TagUseLowerCase, false, "");
				}
			}
			
			// Adding the content
			if(content.length() > 0){
				// XML akan dibuat tag nya utk yang content != (null atau kosong)
				// Opening tag utk key
				sb.append("<");
				sb.append(finalKey);
				sb.append(">");
				
				sb.append(content);

				logger.debug("Function convertJSONToXMLStringWithKeyAsXMLTag - Adding content for key: " + finalKey + ", content: " + content);

				// Close tag utk key
				sb.append("</");
				sb.append(finalKey);
				sb.append(">");
			}
			
		}
		
		// Closing root tag
		if(withRootTag == true){
			if(rootTag.length()>0){
				sb.append("</");
				sb.append(rootTag);
				sb.append(">");
			}
		}
		
		hasil = sb.toString();

		logger.debug("Function convertJSONToXMLStringWithKeyAsXMLTag - hasil: " + hasil);
		
		return hasil;
	}
	
	public Long getDateDifferent(Date firstDate, Date secondDate){
		Long hasil = 0L;
		
		Long lFirstDate = firstDate.getTime();
		Long lSecondDate = secondDate.getTime();
		
		hasil = Math.abs(TimeUnit.MILLISECONDS.toHours(lFirstDate - lSecondDate));
		
		return hasil;
	}
		
	public void printAllJSONContents(Integer indent, Object jsonData){
		String titikIndent = " . ";
		
		for(int indentation = 1; indentation <= indent; indentation++){
			titikIndent = titikIndent + " . ";
		}
		
		if(jsonData instanceof JSONObject){
			// Print the key
			JSONObject theJSONObject = (JSONObject) jsonData;
			Iterator<?> itrJSONObject = theJSONObject.keys();
			
			while(itrJSONObject.hasNext()){
				String theKey = (String) itrJSONObject.next();
				
				// Get the content
				String theContent = "";
				if(theJSONObject.get(theKey) instanceof JSONObject){
					theContent = theJSONObject.getJSONObject(theKey).toString();
				} else if(theJSONObject.get(theKey) instanceof String){
					theContent = theJSONObject.getString(theKey);
				} else if(theJSONObject.get(theKey) instanceof Integer){
					theContent = Integer.toString(theJSONObject.getInt(theKey));
				} else if(theJSONObject.get(theKey) instanceof Long){
					theContent = Long.toString(theJSONObject.getLong(theKey));
				} else if(theJSONObject.get(theKey) instanceof Boolean){
					theContent = Boolean.toString(theJSONObject.getBoolean(theKey));
				} else if(theJSONObject.get(theKey) instanceof JSONArray){
					theContent = theJSONObject.getJSONArray(theKey).toString();
				}
				
				logger.debug(titikIndent + theKey + " : " + theContent);
				
				if((theJSONObject.get(theKey) instanceof JSONObject) || (theJSONObject.get(theKey) instanceof JSONArray)){
					// Reprint if the content is still JSONObject or JSONArray
					printAllJSONContents(indent + 1, theJSONObject.get(theKey));
				}
			}
		} else if(jsonData instanceof JSONArray){
			JSONArray theJSONArray = (JSONArray) jsonData;
			
			String theContent = "";
			for(int x = 0; x < theJSONArray.length(); x++){
				if(theJSONArray.get(x) instanceof String){
					theContent = theJSONArray.getString(x);

					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : " + theContent);
				} else if(theJSONArray.get(x) instanceof Integer){
					theContent = Integer.toString(theJSONArray.getInt(x));

					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : " + theContent);
				} else if(theJSONArray.get(x) instanceof Long){
					theContent = Long.toString(theJSONArray.getLong(x));

					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : " + theContent);
				} else if(theJSONArray.get(x) instanceof Boolean){
					theContent = Boolean.toString(theJSONArray.getBoolean(x));

					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : " + theContent);
				} else if(theJSONArray.get(x) instanceof JSONObject){
					// reprint
					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : ");

					printAllJSONContents(indent + 1, theJSONArray.getJSONObject(x));
				} else if(theJSONArray.get(x) instanceof JSONArray){
					// reprint
					logger.debug(titikIndent + "array index: " + Integer.toString(x) + " : ");

					printAllJSONContents(indent + 1, theJSONArray.getJSONArray(x));
				}				
			}
		} else {
			// Not compatible
			logger.debug("Print JSON content is failed, not compatible!");
		}
	}
	
	
	public Date setTimeInDate(final Date date, final int hourOfDay, final int minute, final int second, final int millisecond){
		final GregorianCalendar gC = new GregorianCalendar();
		
		gC.setTime(date);
		gC.set(Calendar.HOUR_OF_DAY, hourOfDay);
		gC.set(Calendar.MINUTE, minute);
		gC.set(Calendar.SECOND, second);
		gC.set(Calendar.MILLISECOND, millisecond);
		
		Date finalSetDateTime = gC.getTime();
		
		logger.debug("Function setTimeInDate - finalSetDateTime: " + finalSetDateTime.toString());
		
		return finalSetDateTime;
	}
	
	public Date setDayInMonth(final Date date, final int dayOfMonth, final int hourOfDay, final int minute, final int second, final int millisecond){
		final GregorianCalendar gC = new GregorianCalendar();
		
		gC.setTime(date);
		gC.set(Calendar.DAY_OF_MONTH, dayOfMonth);
		gC.set(Calendar.HOUR_OF_DAY, hourOfDay);
		gC.set(Calendar.MINUTE, minute);
		gC.set(Calendar.SECOND, second);
		gC.set(Calendar.MILLISECOND, millisecond);
		
		Date finalSetDateTime = gC.getTime();
		
		logger.debug("Function setDayInMonth - finalSetDateTime: " + finalSetDateTime);
		
		return finalSetDateTime;
	}
	

	public Date getLastPeriodMonths(final int monthDiff){
		// monthDiff can be - (negative) for previous months or + (positif) for next months
		// in calendar, month is started from 0
		
		Calendar cal = Calendar.getInstance();
		
		cal.add(Calendar.MONTH, monthDiff);
		
		logger.debug("Function getLastPeriodMonths - monthDiff " + Integer.toString(monthDiff) + " from this month is " + cal.getTime().toString());
		
		return cal.getTime();
	}
	
	public Date getLastPeriodMonthBegining(final int monthDiff){
		Date theLastMonth = getLastPeriodMonths(monthDiff);
		
		Date theEarlyDateTimeOfTheLastMonth = setDayInMonth(theLastMonth, Calendar.getInstance().getActualMinimum(Calendar.DAY_OF_MONTH), Calendar.getInstance().getActualMinimum(Calendar.HOUR_OF_DAY), 0, 0, 0);
		
		logger.debug("Function getLastPeriodMonthBegining - first day of monthDiff " + Integer.toString(monthDiff) + " from today is " + theEarlyDateTimeOfTheLastMonth.toString());
		
		return theEarlyDateTimeOfTheLastMonth;
	}
	
	public Date getLastPeriodMonthEnding(final int monthDiff){
		Date theLastMonth = getLastPeriodMonths(monthDiff);
		
		Date theLastDateTimeOfTheLastMonth = setDayInMonth(theLastMonth, Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH), Calendar.getInstance().getActualMaximum(Calendar.HOUR_OF_DAY), 59, 59, 999);
		
		logger.debug("Function getLastPeriodMonthEnding - last day of monthDiff " + Integer.toString(monthDiff) + " from today is " + theLastDateTimeOfTheLastMonth.toString());
		
		return theLastDateTimeOfTheLastMonth;
	}
	
	public int getMinuteDifference(Date earlierDate, Date laterDate){
		int diff = 0;
		
		if(earlierDate == null || laterDate == null) return 0;
		
		diff = (int) ((laterDate.getTime()/60000) - (earlierDate.getTime()/60000));
		
		return diff;
	}
	
	public Date getInBetweenTwoDates(Date dateOne, Date dateTwo){
		// Mendapatkan tanggal lengkap di tengah2 antara 2 datetime.
		int minutesDiff = getMinuteDifference(dateOne, dateTwo);
		int halfMinutesDiff = minutesDiff / 2;
		
		// Add dateOne with halfMinutesDiff
		final long ONE_MINUTE_IN_MILLIS = 60000;
		
		long dateOneInMillis = dateOne.getTime();
		
		Date dateOneAdded = new Date(dateOneInMillis + (halfMinutesDiff * ONE_MINUTE_IN_MILLIS));
		
		return dateOneAdded;
	}
	
	public String convertIntToDigitString(int digitLength, int theInt){
		if(theInt >= 0){
			String strTheInt = Integer.toString(theInt);
			
			if(strTheInt.length() <= digitLength){
				// Do the process
				String hasil = "";
				for(int x = 0; x < (digitLength - strTheInt.length()); x++){
					hasil = hasil + "0";
				}
				hasil = hasil + strTheInt;
				
				return hasil;
			} else {
				// Failed, since theInt has length more than digitLength. 
				// Just return strTheInt then
				return strTheInt;
			}
		} else {
			// For negative value
			theInt = theInt * -1;
			
			String strTheInt = Integer.toString(theInt);
			
			if(strTheInt.length() <= digitLength){
				// Do the process
				String hasil = "";
				for(int x = 0; x < (digitLength - strTheInt.length()); x++){
					hasil = hasil + "0";
				}
				hasil = hasil + strTheInt;
				
				// Add the minus sign
				hasil = "-" + hasil;
				
				return hasil;
			} else {
				// Failed, since theInt has length more than digitLength. 
				// Just return strTheInt then
				return "-" + strTheInt;
			}
			
		}
	}
	
	public String formatXML(String input)
	{
	    try
	    {
	        final InputSource src = new InputSource(new StringReader(input));
	        final Node document = DocumentBuilderFactory.newInstance()
	                .newDocumentBuilder().parse(src).getDocumentElement();

	        final DOMImplementationRegistry registry = DOMImplementationRegistry
	                .newInstance();
	        final DOMImplementationLS impl = (DOMImplementationLS) registry
	                .getDOMImplementation("LS");
	        final LSSerializer writer = impl.createLSSerializer();

	        writer.getDomConfig().setParameter("format-pretty-print",
	                Boolean.TRUE);
	        writer.getDomConfig().setParameter("xml-declaration", true);

	        return writer.writeToString(document);
	    } catch (Exception e)
	    {
	        e.printStackTrace();
	        return input;
	    }
	}
	
	public JSONObject convertHashMapToJSONObject(HashMap<String, String> mapIn){
		JSONObject jsonHasil = new JSONObject();

		Iterator<?> itrMap = mapIn.entrySet().iterator();
		
		while(itrMap.hasNext()){
			@SuppressWarnings("unchecked")
			Map.Entry<String, String> pair = (Map.Entry<String, String>) itrMap.next();
			
			jsonHasil.put(pair.getKey(), pair.getValue());
		}
		
		return jsonHasil;
	}


	public String convertOneDimensionArrayToString(Object theArray){
		String hasil = "[";
		
		if(theArray instanceof String[]){
			String[] arrStr = (String[]) theArray;
			
			for(int x = 0; x < arrStr.length; x++){
				if(x == 0){
					hasil = hasil + arrStr[x]; 
				} else {
					hasil = hasil + ", " + arrStr[x]; 
				}
			}
		} else if(theArray instanceof Integer[]){
			Integer[] arrStr = (Integer[]) theArray;
			
			for(int x = 0; x < arrStr.length; x++){
				if(x == 0){
					hasil = hasil + Integer.toString(arrStr[x]); 
				} else {
					hasil = hasil + ", " + Integer.toString(arrStr[x]); 
				}
			}
		}
		
		hasil = hasil + "]";
		
		return hasil;		
	}
	
	private Cipher getCipher(int cipherMode, String encryptionKey) throws Exception
    {
        String encryptionAlgorithm = "AES";
        SecretKeySpec keySpecification = new SecretKeySpec(encryptionKey.getBytes("UTF-8"), encryptionAlgorithm);
        Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
        cipher.init(cipherMode, keySpecification);

        return cipher;
    }
	
	public String encryptAES128(String theString, String theKey){
		// theString is string to be encrypted
		// theKey is the encryption key
		
		Cipher cipher;
		byte[] encryptedBytes = null;
		
		try {
			cipher = getCipher(Cipher.ENCRYPT_MODE, theKey);
	        encryptedBytes = cipher.doFinal(theString.getBytes());
		} catch (Exception e) {
			logger.debug("Function encryptAES128 - Exception is raised while encrypting AES128. Exception: " + e);
			logger.error("Exception is raised while encrypting AES128. Exception: " + e);
			logger.info("Exception is raised while encrypting AES128.");
		}

        return org.apache.commons.codec.binary.Base64.encodeBase64String(encryptedBytes);
	}
	
	public String decryptAES128(String theString, String theKey){
		// theString is encrypted String to be decrypted
		// theKey is encryption key
		
        Cipher cipher;
        byte[] plainBytes = null;
        
		try {
			cipher = getCipher(Cipher.DECRYPT_MODE, theKey);
	        plainBytes = cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(theString));
		} catch (Exception e) {
			logger.debug("Function decryptAES128 - Exception is raised while decrypting AES128. Exception: " + e);
			logger.error("Exception is raised while decrypting AES128. Exception: " + e);
			logger.info("Exception is raised while decrypting AES128. Exception: " + e);
		}

        return new String(plainBytes);
    }
	
	public String getStringFromJSONObject(JSONObject jsonMessage, String keyPar){
		String hasil = "";
		
		if(!jsonMessage.isNull(keyPar)){
			if(jsonMessage.get(keyPar) instanceof Integer){
				hasil = Integer.toString(jsonMessage.getInt(keyPar));
			} else if(jsonMessage.get(keyPar) instanceof Long){
				hasil = Long.toString(jsonMessage.getLong(keyPar));
			} else if(jsonMessage.get(keyPar) instanceof Double){
				hasil = String.format("%.2f", jsonMessage.getDouble(keyPar));
			} else {
				hasil = jsonMessage.getString(keyPar);
			}
		} else {
			hasil = "";
		}
		
		return hasil;
	}

	public String printBytesMessage(byte[] message){
		String hasil = "";
		
		for(int i = 0; i < message.length; i++){
			hasil = hasil + (char) message[i];
		}
		
		return hasil;
	}
	
	 /** Read the object from Base64 string. */
	 public Object serializeBackFromString(String s) throws IOException, ClassNotFoundException {
		 byte [] data = Base64.getDecoder().decode(s);
		 ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
	     Object o  = ois.readObject();
	     ois.close();
	     return o;
	 }

	 /** Write the object to a Base64 string. */
	 public String serializeToString(Serializable o) throws IOException {
		 ByteArrayOutputStream baos = new ByteArrayOutputStream();
		 ObjectOutputStream oos = new ObjectOutputStream(baos);
		 oos.writeObject(o);
		 oos.close();
		 return Base64.getEncoder().encodeToString(baos.toByteArray()); 
	 }
	 
	 public Date getStartOfHour(Date theDateTime){
		 Date startOfHour = null;
		 
		 SimpleDateFormat sdfPastiDateTime = new SimpleDateFormat("yyMMddHHmmss");
		 
		 String formattedStartDate = sdfStartEndHour.format(theDateTime);
		 logger.debug("Function getStartOfHour - formattedStartDate: " + formattedStartDate);
		 
		 // Change to yyMMddHHmmSS
		 formattedStartDate = formattedStartDate + "0000";
		 
		 try {
			 startOfHour = sdfPastiDateTime.parse(formattedStartDate);
			 logger.debug("Function getStartOfHour - formattedStartDate Final: " + formattedStartDate);
		 } catch (Exception e) {
			 logger.debug("Function getStartOfHour - Failed to convert theDate to startHourDateTime.");
		 }
		 
		 return startOfHour;
	 }
	 
	public Date getStartOfDate(Date theDate){
		Date startOfDate = null;

		SimpleDateFormat sdfPastiDateTime = new SimpleDateFormat("yyMMddHHmmss");
		SimpleDateFormat sdfStartEndDate = new SimpleDateFormat("yyMMdd");


		String formattedStartDate = sdfStartEndDate.format(theDate);
		logger.debug("Function getStartOfDate - formattedStartDate: " + formattedStartDate);
			
		// Change to yyMMddHHmmSS
		formattedStartDate = formattedStartDate + "000000";
			
		try {	
			startOfDate = sdfPastiDateTime.parse(formattedStartDate);
			logger.debug("Function getStartOfDate - startOfDate final: " + startOfDate.toString());
		} catch (ParseException e) {
			logger.debug("Function getStartOfDate - Failed to convert theDate to startDate");
			e.printStackTrace();
		}
			
		return startOfDate;
	}
		
	public Date getEndOfDate(Date theDate){
		Date endOfDate = null;
		
		System.out.println("theDate: " + theDate.toString());
		
		SimpleDateFormat sdfPastiDateTime = new SimpleDateFormat("yyMMddHHmmss");
		SimpleDateFormat sdfStartEndDate = new SimpleDateFormat("yyMMdd");

		String formattedEndDate = sdfStartEndDate.format(theDate);
		logger.debug("Function getEndOfDate - formattedEndDate: " + formattedEndDate);
			
		// Change to yyMMddHHmmss
		formattedEndDate = formattedEndDate + "235959";
			
		try{
			endOfDate = sdfPastiDateTime.parse(formattedEndDate);
			logger.debug("Function getEndOfDate - endOfDate final: " + endOfDate.toString());
		} catch(ParseException e){
			logger.debug("Function getEndOfDate - Failed to convert theDate to endDate");
			e.printStackTrace();
		}
			
		return endOfDate;
	}
	
	public Date getStartOfYear(Date theDate){
		Date startOfYearDate = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yy"); // Ambil tahunnya saja
		SimpleDateFormat sdfFormatted = new SimpleDateFormat("yyMMddHHmmss");
		
		try {
			String formattedStartOfYear = sdf.format(new Date()) + "0101000000"; // yyMMddHHmmss
			logger.debug("Function getStartOfYear - now Date: " + sdfFormatted.format(new Date()) + ", formattedStartOfYear: " + formattedStartOfYear);

			startOfYearDate = sdfFormatted.parse(formattedStartOfYear);
		} catch (ParseException e) {
			logger.debug("Function getStartOfYear - Failed to get start of year. Error occured.");
		}
		
		return startOfYearDate;
	}
	
	public Date getStartOfMonth(Date theDate){
		Date startOfMonthDate = null;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyMM"); // Ambil tahun dan bulan
		SimpleDateFormat sdfFormatted = new SimpleDateFormat("yyMMddHHmmss");
		
		try{
			String formattedStartOfMonth = sdf.format(new Date()) + "01000000"; // yyMMddHHmmss
			logger.debug("Function getStartOfMonth - now Date: " + sdfFormatted.format(new Date()) + ", formattedStartOfMonth: " + formattedStartOfMonth);
			
			startOfMonthDate = sdfFormatted.parse(formattedStartOfMonth);
		} catch (Exception e) {
			logger.debug("Function getStartOfMonth - Failed to get start of year. Error occured.");
		}
		
		return startOfMonthDate;
	}
	
	public Date getStartOfWeek(Date theDate){
		Date startOfWeekDate = null;
		
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.setTime(theDate);
		
		while(calendar.get(Calendar.DAY_OF_WEEK) > calendar.getFirstDayOfWeek()){
			calendar.add(Calendar.DATE, -1);
		}
		
		startOfWeekDate = calendar.getTime();
		logger.debug("Function getStartOfWeek - theDate: " + theDate.toString() + ", startOfWeekDate: " + startOfWeekDate.toString());
		
		return startOfWeekDate;
	}
	
	public Date addDayDate(int deltaDate, Date theRefDate){
		// last x days before date => deltaDate -x
		// next x days after date => deltaDate +x
		Date hasil = new Date();
		
		Calendar c = Calendar.getInstance();
		c.setTime(theRefDate);
		c.add(Calendar.DATE, deltaDate);
		hasil.setTime(c.getTime().getTime());
		
		return hasil;
	}
	
	public Date addMinuteDate(int deltaMinute, Date theRefDate){
		final long ONE_MINUTE_IN_MILIS = 60000; // miliseconds
		
		long milRefDate = theRefDate.getTime();
		
		Date dateAfterAddMinute = new Date(milRefDate + (deltaMinute * ONE_MINUTE_IN_MILIS));
//		logger.debug("Function addMinuteDate - Date before: " + theRefDate.toString() + ", Date after add " + deltaMinute + " minutes: " +
//				dateAfterAddMinute.toString());
		
		return dateAfterAddMinute;
	}
	
	public boolean isDateBetweenDates(Date startDateRangeIncluded, Date endDateRangeExcluded, Date theDate){
		return theDate.equals(startDateRangeIncluded) || (theDate.after(startDateRangeIncluded) && theDate.before(endDateRangeExcluded));
	}
	
	public List<Date> getTimeLinePeriod(Date startDate, Date endDate, int minuteDelta){
		List<Date> listTimeLine = new ArrayList<Date>();
		
//		SimpleDateFormat sdfPastiDateTime = new SimpleDateFormat("yyMMddHHmmss");

		Date theDate = startDate;
		
		while(theDate.before(endDate)){
			listTimeLine.add(theDate);
//			logger.debug("Function getTimeLinePeriod - Putting theDate " + sdfPastiDateTime.format(theDate) + " into listTimeLine.");
			
			theDate = addMinuteDate(minuteDelta, theDate);
		}
		
		return listTimeLine;
	}
	
	public String[] pushStringIntoFixArray(String[] origArray, int maxLength, String pushedString){
		logger.debug("Function pushStringIntoFixArray - origArray: " + Arrays.toString(origArray) + ", maxLength: " + maxLength + ", pushedString: " + pushedString);

		if(origArray.length < maxLength){
			// Legacy origArray length < maxLength
			String[] penampung = new String[origArray.length + 1]; // 1 is slot for pushedString
			logger.debug("Function pushStringIntoFixArray - Initiate penampung dengan length: " + penampung.length);
			
			// Copy semua isi origArray ke penampung
			System.arraycopy(origArray, 0, penampung, 0, origArray.length);
			logger.debug("Function pushStringIntoFixArray - After copy penampung: " + Arrays.toString(penampung));
			
			// Masukkan pushedString ke penamping
			penampung[origArray.length] = pushedString; // pushedString dimasukkan ke slot paling belakang
			logger.debug("Function pushStringIntoFixArray - Final penampung: " + Arrays.toString(penampung));
			
			// Return penampung
			return penampung;
		} else if(origArray.length == maxLength){
			// Initiate penampung
			String[] penampung = new String[maxLength];
			logger.debug("Function pushStringIntoFixArray - Initiate penampung dengan length: " + penampung.length);
			
			// Copy sejak posisi 1 dari origArray (slot paling depan/kiri dibuang dari origArray) ke penampung di posisi penamping 0
			System.arraycopy(origArray, 1, penampung, 0, maxLength - 1); // Geser ke kiri 1, yang paling kiri dibuang, sisakan slot paling kanan buat pushedString
			
			// Masuki pushedString ke posisi terakhir
			penampung[maxLength-1] = pushedString;
			logger.debug("Function pushStringIntoFixArray - Final penampung: " + Arrays.toString(penampung));

			// Return penampung
			return penampung;
		} else {
			// Initiate penampung
			String[] penampung = new String[maxLength];
			logger.debug("Function pushStringIntoFixArray - Initiate penampung dengan length: " + penampung.length);
			
			// Cut origArray menjadi origArraylength == maxLength, masukkan ke penampung
			int selisihSlot = origArray.length - maxLength;
			logger.debug("Function pushStringIntoFixArray - selisihSlot: " + selisihSlot);
			
			System.arraycopy(origArray, selisihSlot, penampung, 0, maxLength);
			logger.debug("Function pushStringIntoFixArray - After copy - penampung: " + Arrays.toString(penampung));

			// Pindahkan 1 slot ke kiri
			System.arraycopy(penampung, 1, penampung, 0, penampung.length - 1);
			
			// Masukkan pushedString ke penampung paling kanan
			penampung[maxLength -1] = pushedString;
			logger.debug("Function pushStringIntoFixArray - Final penampung: " + Arrays.toString(penampung));
			
			// Return penampung
			return penampung;
		}
	}
	
	// Update 31 Mar 2019
	public HashMap<String, String> hitHTTPSGetGeneral(String traceCode, String URL, String getParameter, JSONObject jsonHeaders){
		System.out.println("--------------- HTTPS GET -------------------");
		if(jsonHeaders != null){
			logger.debug("Function hitHTTPSGetGeneral - Incoming parameter - url: " + URL + ", postParameter: " + getParameter + ", headers: " + jsonHeaders.toString());
		} else {
			logger.debug("Function hitHTTPSGetGeneral - Incoming parameter - url: " + URL + ", postParameter: " + getParameter);
		}
		
		SimpleDateFormat sdfWithMiliSecond = new SimpleDateFormat("yyMMddHHmmssSSS");
		
		HashMap<String, String> mapHasil = new HashMap<String, String>();

		// Parameters
		// Request
		String hitRequestBody = getParameter;
		String hitRequestURL = "";
		Date hitRequestDateTime = new Date();
		
		// HeaderRequest
		JSONObject hitRequestHttpHeader = new JSONObject();
		if(jsonHeaders != null){
			hitRequestHttpHeader = jsonHeaders;
		}
		
		// Response
		String hitResultStatus = "000";
		String hitResultStatusDescription = "";
		int hitResultHttpStatusCode = 0;
		String hitResultBody = "";
		Date hitResultDateTime = new Date();
		
		// Header Response
		JSONObject hitResultHttpHeader = new JSONObject();
				
		CloseableHttpClient httpclient = null;
		try{
			// Create self signed certificate
//			SSLContextBuilder builder = new SSLContextBuilder();
//		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
//		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
//		            builder.build());
//		    httpclient = HttpClients.custom().setSSLSocketFactory(
//		            sslsf).build();
			
			final SSLContext sslContext = new SSLContextBuilder()
				    .loadTrustMaterial(null, (x509CertChain, authType) -> true).build();
			
			httpclient = HttpClients.custom()
				    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
				    .setSSLContext(sslContext).build();

		    if (getParameter.length() > 0) {
		    	hitRequestURL = URL + "?" + URLEncoder.encode(getParameter, StandardCharsets.UTF_8.toString());
		    } else {
		    	hitRequestURL = URL;
		    }
		    
			System.out.println("----- hitRequestURL: " + hitRequestURL);
			
			HttpGet get = new HttpGet(hitRequestURL);
			System.out.println("----- get URL: " + get.getURI());
			
			// Set Headers
			if(jsonHeaders != null){
				Iterator<String> keys = jsonHeaders.keys();
				
				while(keys.hasNext()){
					String key = keys.next();
					String val = getStringFromJSONObject(jsonHeaders, key);
					
					get.setHeader(key, val);
				}
			}

			// Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
				@Override
				public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
                    JSONObject jsonResponse = new JSONObject();
                    
                    // Get Headers in the response
                    JSONObject jsonHeaderResponse = new JSONObject();
                    Header[] headers = response.getAllHeaders();
                    for(Header header : headers){
                    	jsonHeaderResponse.put(header.getName(), header.getValue());
                    }

                    int statusCode = response.getStatusLine().getStatusCode();
                    System.out.println("----- resp. statusCode: " + Integer.toString(statusCode));
                    
                    if (statusCode >= 200 && statusCode < 300) {
                        HttpEntity entity = response.getEntity();
                        
                        String HTTPBodyResponse = "";
                        if(entity != null){
                        	HTTPBodyResponse = EntityUtils.toString(entity);
                        }
                        System.out.println("----- HTTPBodyResponse: " + HTTPBodyResponse);
                        
                        jsonResponse.put("HTTPStatus", statusCode);
                        jsonResponse.put("HTTPHeaderResponse", jsonHeaderResponse);
                        jsonResponse.put("HTTPBodyResponse", HTTPBodyResponse);
                        
            			logger.debug(traceCode + " - Function hitHTTPSGetGeneral - Hit vendor SUCCESS. HTTP STATUS: " + 
                        		Integer.toString(statusCode) + ", BODY: " + HTTPBodyResponse);
                    } else {
            			logger.debug(traceCode + " - Function hitHTTPSGetGeneral - Hit vendor FAILED. HTTP STATUS: " + 
                        		Integer.toString(statusCode));

                        //throw new ClientProtocolException("Unexpected response status: " + statusCode);
                        jsonResponse.put("HTTPStatus", statusCode);
                        jsonResponse.put("HTTPHeaderResponse", jsonHeaderResponse);
                        jsonResponse.put("HTTPBodyResponse", "");
                    }
                    
                    return jsonResponse.toString();
				}
            };
            
            // Execute GET
            hitRequestDateTime = new Date();
            String response = httpclient.execute(get, responseHandler);
            hitResultDateTime = new Date();
            
            // response is actually in JSONObject
            JSONObject jsonResponse = new JSONObject(response);
            
            // Prepare response parameter
            hitResultHttpStatusCode = jsonResponse.getInt("HTTPStatus");
            hitResultBody = getStringFromJSONObject(jsonResponse, "HTTPBodyResponse");
			
			// Put into mapHasil
            mapHasil.put("hitRequestBody", hitRequestBody);
            mapHasil.put("hitRequestURL", hitRequestURL);
            mapHasil.put("hitRequestDateTime", sdfWithMiliSecond.format(hitRequestDateTime));
            mapHasil.put("hitRequestHttpHeader", hitRequestHttpHeader.toString());
            
            mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultStatusDescription", hitResultStatusDescription);
            mapHasil.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
            mapHasil.put("hitResultBody", hitResultBody);
            mapHasil.put("hitResultDateTime", sdfWithMiliSecond.format(hitResultDateTime));
            mapHasil.put("hitResultHttpHeader", hitResultHttpHeader.toString());
            
			mapHasil.put("hitStatus", "SUCCESS");            
		} catch (Exception e) {
			e.printStackTrace();
			
			logger.debug(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e);
			logger.error(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e);
			logger.info(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e.toString());
			
			hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
		} finally {
			try {
				httpclient.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return mapHasil;
	}
	
	// Update 25 Nov 2018
	public HashMap<String, String> hitHTTPGetGeneral(String traceCode, String URL, String getParameter, JSONObject jsonHeaders){
		System.out.println("--------------- HIT HTTP -----------------");
		if(jsonHeaders != null){
			logger.debug("Function hitHTTPGetGeneral - Incoming parameter - url: " + URL + ", postParameter: " + getParameter + ", headers: " + jsonHeaders.toString());
		} else {
			logger.debug("Function hitHTTPGetGeneral - Incoming parameter - url: " + URL + ", postParameter: " + getParameter);
		}
		
		SimpleDateFormat sdfWithMiliSecond = new SimpleDateFormat("yyMMddHHmmssSSS");
		
		HashMap<String, String> mapHasil = new HashMap<String, String>();

		// Parameters
		// Request
		String hitRequestBody = URL + "?" + getParameter;
		String hitRequestURL = "";
		Date hitRequestDateTime = new Date();
		
		// HeaderRequest
		JSONObject hitRequestHttpHeader = new JSONObject();
		if(jsonHeaders != null){
			hitRequestHttpHeader = jsonHeaders;
		}
		
		// Response
		String hitResultStatus = "000";
		String hitResultStatusDescription = "";
		int hitResultHttpStatusCode = 0;
		String hitResultBody = "";
		Date hitResultDateTime = new Date();
		
		// Header Response
		JSONObject hitResultHttpHeader = new JSONObject();
				
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		try{
			hitRequestURL = URL + "?" + getParameter;
			System.out.println("hitRequestURL: " + hitRequestURL);
			
			HttpGet get = new HttpGet(hitRequestURL);
			
			// Set Headers
			if(jsonHeaders != null){
				Iterator<String> keys = jsonHeaders.keys();
				
				while(keys.hasNext()){
					String key = keys.next();
					String val = getStringFromJSONObject(jsonHeaders, key);
					
					get.setHeader(key, val);
				}
			}

			// Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
				@Override
				public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
                    JSONObject jsonResponse = new JSONObject();
                    
                    // Get Headers in the response
                    JSONObject jsonHeaderResponse = new JSONObject();
                    Header[] headers = response.getAllHeaders();
                    for(Header header : headers){
                    	jsonHeaderResponse.put(header.getName(), header.getValue());
                    }

                    int statusCode = response.getStatusLine().getStatusCode();
                    
                    if (statusCode >= 200 && statusCode < 300) {
                        HttpEntity entity = response.getEntity();
                        
                        String HTTPBodyResponse = "";
                        if(entity != null){
                        	HTTPBodyResponse = EntityUtils.toString(entity);
                        }
                        System.out.println("HTTPBodyResponse: " + HTTPBodyResponse);
                        
                        jsonResponse.put("HTTPStatus", statusCode);
                        jsonResponse.put("HTTPHeaderResponse", jsonHeaderResponse);
                        jsonResponse.put("HTTPBodyResponse", HTTPBodyResponse);
                        
            			logger.debug(traceCode + " - Function hitHTTPGetGeneral - Hit vendor SUCCESS. HTTP STATUS: " + 
                        		Integer.toString(statusCode) + ", BODY: " + HTTPBodyResponse);
                    } else {
            			logger.debug(traceCode + " - Function hitHTTPGetGeneral - Hit vendor FAILED. HTTP STATUS: " + 
                        		Integer.toString(statusCode));

                        throw new ClientProtocolException("Unexpected response status: " + statusCode);                        
                    }
                    
                    return jsonResponse.toString();
				}
            };
            
            // Execute GET
            hitRequestDateTime = new Date();
            String response = httpclient.execute(get, responseHandler);
            hitResultDateTime = new Date();
            
            // response is actually in JSONObject
            JSONObject jsonResponse = new JSONObject(response);
            
            // Prepare response parameter
            hitResultHttpStatusCode = jsonResponse.getInt("HTTPStatus");
            hitResultBody = getStringFromJSONObject(jsonResponse, "HTTPBodyResponse");
			
			// Put into mapHasil
            mapHasil.put("hitRequestBody", hitRequestBody);
            mapHasil.put("hitRequestURL", hitRequestURL);
            mapHasil.put("hitRequestDateTime", sdfWithMiliSecond.format(hitRequestDateTime));
            mapHasil.put("hitRequestHttpHeader", hitRequestHttpHeader.toString());
            
            mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultStatusDescription", hitResultStatusDescription);
            mapHasil.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
            mapHasil.put("hitResultBody", hitResultBody);
            mapHasil.put("hitResultDateTime", sdfWithMiliSecond.format(hitResultDateTime));
            mapHasil.put("hitResultHttpHeader", hitResultHttpHeader.toString());
            
			mapHasil.put("hitStatus", "SUCCESS");            
		} catch (Exception e) {
			e.printStackTrace();
			
			logger.debug(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e);
			logger.error(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e);
			logger.info(traceCode + " - Function hitClientGetGeneral - System error while querying vendor HTTP GET server. Error: " + e.toString());
			
			hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
		} finally {
			try {
				httpclient.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return mapHasil;
	}

	// Update 4 APR 2019
	public HashMap<String, String> hitHTTPSPostFormUrl(String messageId, String url, List<NameValuePair> formParameters, JSONObject jsonHeaders){
		HashMap<String, String> mapHasil = new HashMap<String, String>();
		
		LocalDateTime now = LocalDateTime.now();
		
		CloseableHttpClient httpclient = null;
		
		try {
			// Create self signed certificate
			SSLContextBuilder builder = new SSLContextBuilder();
		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
		            builder.build());
		    httpclient = HttpClients.custom().setSSLSocketFactory(
		            sslsf).build();

			HttpPost post = new HttpPost(url);

			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formParameters, Consts.UTF_8);

			// Set Parameter
			post.setEntity(entity);
			
			// Set Content Type
			post.setHeader("Content-Type", "application/x-www-form-urlencoded");
			post.addHeader("charset", "UTF-8");
			
			CloseableHttpResponse response = httpclient.execute(post);
			
			// Get HTTP Response Status Code
			int statusCode = response.getStatusLine().getStatusCode();
			mapHasil.put("hitStatusCode", Integer.toString(statusCode));
			logger.debug("Function hitHTTPPost - Incoming parameter - response statusCode: " + Integer.toString(statusCode));

			// Get HTTP Response Header
			// The headers response will be stored in JSON String
			JSONObject jsonHeader = new JSONObject();
			Header headers[] = response.getAllHeaders();
			for(Header h:headers){
				jsonHeader.put(h.getName(), h.getValue());
			}
			mapHasil.put("hitHeaderResult", jsonHeader.toString());
			logger.debug("Function hitHTTPPost - Incoming parameter - response jsonHeader: " + jsonHeader.toString());
			
			// Get HTTP Response Body as String
			String hasilHit = EntityUtils.toString(response.getEntity());
            mapHasil.put("hitBodyResult", hasilHit);
			logger.debug("Function hitHTTPPost - Incoming parameter - hasilHitBodyResponse: " + hasilHit);

			// Put into mapHasil
            mapHasil.put("hitRequestBody", formParameters.toString());
            mapHasil.put("hitRequestURL", url);
            mapHasil.put("hitRequestDateTime", now.format(formatter));
            mapHasil.put("hitRequestHttpHeader", "");
            
            // Get nowest now
    		now = LocalDateTime.now();

            mapHasil.put("hitResultStatus", "000");
            mapHasil.put("hitResultStatusDescription", "HIT OK");
            mapHasil.put("hitResultHttpStatusCode", Integer.toString(statusCode));
            mapHasil.put("hitResultBody", hasilHit);
            mapHasil.put("hitResultDateTime", now.format(formatter));
            mapHasil.put("hitResultHttpHeader", jsonHeader.toString());
            
			mapHasil.put("hitStatus", "SUCCESS");            
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor. Error: " + e.toString());
			
			String hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", "");
		} catch (NoSuchAlgorithmException e) {
			logger.debug("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor. Error: " + e.toString());
			
			String hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", "");
		} catch (KeyStoreException e) {
			logger.debug("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor. Error: " + e.toString());
			
			String hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", "");
		} catch (KeyManagementException e) {
			logger.debug("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor. Error: " + e.toString());
			
			String hitResultStatus = "201";
			mapHasil.put("hitStatus", "FAILED");
			mapHasil.put("hitResultStatus", hitResultStatus);
            mapHasil.put("hitResultHttpStatusCode", "");
		} finally {
			try {
				httpclient.close();
			} catch (IOException e) {
				logger.debug("Function hitClientPost - System error while closing the httpclient. Error: " + e);
				logger.error("Function hitClientPost - System error while closing the httpclient. Error: " + e);
				logger.info("Function hitClientPost - System error while closing the httpclient. Error: " + e.toString());
			}
		}		

		return mapHasil;
	}

	// Update 25 Nov 2018
	public HashMap<String, String> hitHTTPPostGeneralNonForm(String traceCode, String URL, String postParameter, JSONObject jsonHeaders){
		if(jsonHeaders != null){
			logger.debug("Function hitHTTPPostGeneralNonForm - Incoming parameter - url: " + URL + ", postParameter: " + postParameter + ", headers: " + jsonHeaders.toString());
		} else {
			logger.debug("Function hitHTTPPostGeneralNonForm - Incoming parameter - url: " + URL + ", postParameter: " + postParameter);
		}
		
		SimpleDateFormat sdfWithMiliSecond = new SimpleDateFormat("yyMMddHHmmssSSS");

		// Parameters Output
		// Request
		String hitRequestBody = postParameter;
		String hitRequestURL = URL;
		Date hitRequestDateTime = new Date();
		
		// HeaderRequest
		JSONObject hitRequestHttpHeader = new JSONObject();
		if(jsonHeaders != null){
			hitRequestHttpHeader = jsonHeaders;
		}
		
		// Response
		String hitResultStatus = "000";
		String hitResultStatusDescription = "";
		int hitResultHttpStatusCode = 0;
		String hitResultBody = "";
		Date hitResultDateTime = new Date();
		
		// Header Response
		JSONObject hitResultHttpHeader = new JSONObject();

		HashMap<String, String> mapHasilPost = new HashMap<String, String>();
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		hitResultHttpStatusCode = 0;
		try {
			HttpPost post = new HttpPost(URL);
			
			HttpEntity entity = new ByteArrayEntity(hitRequestBody.getBytes("UTF-8"));

			// Set Parameter
			post.setEntity(entity);
			
			// Set Headers
			if(jsonHeaders != null){
				Iterator<String> keys = jsonHeaders.keys();
				
				while(keys.hasNext()){
					String key = keys.next();
					String val = getStringFromJSONObject(jsonHeaders, key);
					
					post.setHeader(key, val);
				}
			}
			
			hitRequestDateTime = new Date();
			CloseableHttpResponse response = httpclient.execute(post);
			hitResultDateTime = new Date();
			
			// Get HTTP Response Status Code
			hitResultHttpStatusCode = response.getStatusLine().getStatusCode();
			logger.debug("Function hitHTTPPostGeneralNonForm - Incoming parameter - response statusCode: " + Integer.toString(hitResultHttpStatusCode));

			// Get HTTP Response Header
			// The headers response will be stored in JSON String
			hitResultHttpHeader = new JSONObject();
			Header headers[] = response.getAllHeaders();
			for(Header h:headers){
				hitResultHttpHeader.put(h.getName(), h.getValue());
			}
			logger.debug("Function hitHTTPPost - Incoming parameter - response jsonHeader: " + hitResultHttpHeader.toString());
			
			// Get HTTP Response Body as String
			hitResultBody = EntityUtils.toString(response.getEntity());
			logger.debug("Function hitHTTPPost - Incoming parameter - hasilHitBodyResponse: " + hitResultBody);

			mapHasilPost.put("hitRequestBody", hitRequestBody);
			mapHasilPost.put("hitRequestURL", hitRequestURL);
			mapHasilPost.put("hitRequestDateTime", sdfWithMiliSecond.format(hitRequestDateTime));
			mapHasilPost.put("hitRequestHttpHeader", hitRequestHttpHeader.toString());
			
			mapHasilPost.put("hitResultStatus", hitResultStatus);
			mapHasilPost.put("hitResultStatusDescription", hitResultStatusDescription);
			mapHasilPost.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
			mapHasilPost.put("hitResultBody", hitResultBody);
			mapHasilPost.put("hitResultDateTime", sdfWithMiliSecond.format(hitResultDateTime));
			mapHasilPost.put("hitResultHttpHeader", hitResultHttpHeader.toString());
			
			mapHasilPost.put("hitStatus", "SUCCESS");            
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor Dompas. Error: " + e.toString());
			
			hitResultStatus = "201";
            mapHasilPost.put("hitStatus", "FAILED");
			mapHasilPost.put("hitResultStatus", hitResultStatus);
			mapHasilPost.put("hitResultHttpStatusCode", Integer.toString(hitResultHttpStatusCode));
		}
		
		try {
			httpclient.close();
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.error("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.info("Function hitClientPost - System error while closing the httpclient. Error: " + e.toString());
		}

		return mapHasilPost;
	}
	
	public HashMap<String, String> hitHTTPPost(String url, String postParameter, String contentType){
		logger.debug("Function hitHTTPPost - Incoming parameter - url: " + url + ", postParameter: " + postParameter + ", contentType: " + contentType);
		HashMap<String, String> mapHasil = new HashMap<String, String>();
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		try {
			HttpPost post = new HttpPost(url);
			
			HttpEntity entity = new ByteArrayEntity(postParameter.getBytes("UTF-8"));

			// Set Parameter
			post.setEntity(entity);
			
			// Set Content Type
			post.setHeader("Content-Type", contentType);
			
			CloseableHttpResponse response = httpclient.execute(post);
			
			// Get HTTP Response Status Code
			int statusCode = response.getStatusLine().getStatusCode();
			mapHasil.put("hitStatusCode", Integer.toString(statusCode));
			logger.debug("Function hitHTTPPost - Incoming parameter - response statusCode: " + Integer.toString(statusCode));

			// Get HTTP Response Header
			// The headers response will be stored in JSON String
			JSONObject jsonHeader = new JSONObject();
			Header headers[] = response.getAllHeaders();
			for(Header h:headers){
				jsonHeader.put(h.getName(), h.getValue());
			}
			mapHasil.put("hitHeaderResult", jsonHeader.toString());
			logger.debug("Function hitHTTPPost - Incoming parameter - response jsonHeader: " + jsonHeader.toString());
			
			// Get HTTP Response Body as String
			String hasilHit = EntityUtils.toString(response.getEntity());
            mapHasil.put("hitBodyResult", hasilHit);
			logger.debug("Function hitHTTPPost - Incoming parameter - hasilHitBodyResponse: " + hasilHit);

			mapHasil.put("hitCompleteUrl", url);
			mapHasil.put("hitStatus", "SUCCESS");
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor Dompas. Error: " + e.toString());
			
            mapHasil.put("hitStatus", "FAILED");
            mapHasil.put("hitResult", "Error");
		}
		
		try {
			httpclient.close();
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.error("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.info("Function hitClientPost - System error while closing the httpclient. Error: " + e.toString());
		}

		return mapHasil;
	}
	
	
	public HashMap<String, String> hitHTTPPostFormUrl(String url, List<NameValuePair> formParameters){
		HashMap<String, String> mapHasil = new HashMap<String, String>();
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		try {
			HttpPost post = new HttpPost(url);

			UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formParameters, Consts.UTF_8);

			// Set Parameter
			post.setEntity(entity);
			
			// Set Content Type
			post.setHeader("Content-Type", "application/x-www-form-urlencoded");
			post.addHeader("charset", "UTF-8");
			
			CloseableHttpResponse response = httpclient.execute(post);
			
			// Get HTTP Response Status Code
			int statusCode = response.getStatusLine().getStatusCode();
			mapHasil.put("hitStatusCode", Integer.toString(statusCode));
			logger.debug("Function hitHTTPPost - Incoming parameter - response statusCode: " + Integer.toString(statusCode));

			// Get HTTP Response Header
			// The headers response will be stored in JSON String
			JSONObject jsonHeader = new JSONObject();
			Header headers[] = response.getAllHeaders();
			for(Header h:headers){
				jsonHeader.put(h.getName(), h.getValue());
			}
			mapHasil.put("hitHeaderResult", jsonHeader.toString());
			logger.debug("Function hitHTTPPost - Incoming parameter - response jsonHeader: " + jsonHeader.toString());
			
			// Get HTTP Response Body as String
			String hasilHit = EntityUtils.toString(response.getEntity());
            mapHasil.put("hitBodyResult", hasilHit);
			logger.debug("Function hitHTTPPost - Incoming parameter - hasilHitBodyResponse: " + hasilHit);

			mapHasil.put("hitCompleteUrl", url);
			mapHasil.put("hitStatus", "SUCCESS");
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.error("Function hitClientPost - System error while querying vendor Dompas. Error: " + e);
			logger.info("Function hitClientPost - System error while querying vendor Dompas. Error: " + e.toString());
			
            mapHasil.put("hitStatus", "FAILED");
            mapHasil.put("hitResult", "Error");
		}
		
		try {
			httpclient.close();
		} catch (IOException e) {
			logger.debug("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.error("Function hitClientPost - System error while closing the httpclient. Error: " + e);
			logger.info("Function hitClientPost - System error while closing the httpclient. Error: " + e.toString());
		}

		return mapHasil;
	}
	
	public JSONObject MergeJSONObjects(JSONObject json1, JSONObject json2) {
		JSONObject mergedJSON = new JSONObject();
		try {
			mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
			for (String crunchifyKey : JSONObject.getNames(json2)) {
				mergedJSON.put(crunchifyKey, json2.get(crunchifyKey));
			}
 
		} catch (JSONException e) {
			logger.debug("Function MergeJSONObjects - Failed to merge the JSONS. Error: " + e);
			logger.error("Function MergeJSONObjects - Failed to merge the JSONS. Error: " + e);
			logger.info("Failed to merge the JSONS. Error: " + e);
		}
		
		return mergedJSON;
	}
	
	public double getPPH(double parameterFee){
		return parameterFee * 2 / 100;
	}
	
	public double getPPN(double parameterFee){
		return parameterFee * 10 / 100;
	}
	
	public double separatePPNFromFee(double parameterFee){
		return parameterFee - (parameterFee / 1.1);
	}
	
	public String generateUniqueId(String type, int length){
		String uniqueId = generateUniqueID(); // Default
		
		if(type.trim().equals("ALPHANUMERIC")){
			uniqueId = RandomStringUtils.randomAlphanumeric(length);
		} else if(type.trim().equals("ALPHABETICAL")){
			uniqueId = RandomStringUtils.randomAlphabetic(length);
		} else {
			// Default is NUMERIC
			uniqueId = RandomStringUtils.randomNumeric(length);
		}
		
		return uniqueId;
	}
	
	public String generateIdFromStringName(String theName){
		String hasil = "";
		
		// Change to uppercase
		theName = theName.toUpperCase();
		
		// Get 1 first letter, 2 last letter, yymmdd
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
		hasil = theName.substring(0, 1) + theName.substring(theName.length() - 2, theName.length()) + sdf.format(new Date());
		
		return hasil;
	}
	
	public boolean isEmailAddressValid(String emailAddress){
		final Pattern EMAIL_REGEX = Pattern.compile("[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", Pattern.CASE_INSENSITIVE);
		
		return EMAIL_REGEX.matcher(emailAddress).matches();
	}
	
	public String maskStringMessage(String theMessage, String theMask, int viewableStringLength){
		String maskedString = "";
		
		// Cut to length = vieableCharsNo + maskedCharNo
		if(viewableStringLength < theMessage.length()){
			maskedString = theMessage.substring(0, viewableStringLength);

			// Mask!
			maskedString = maskedString + " *** ";
		} else {
			maskedString = theMessage;
		}		
		
		System.out.println("maskedString: " + maskedString);
		return maskedString;
	}
	
	
}
