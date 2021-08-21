package com.blastme.messaging.smsbulk.router;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
//import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.blastme.messaging.configuration.Configuration;
import com.blastme.messaging.toolpooler.ClientPropertyPooler;
import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.LoggingPooler;
import com.blastme.messaging.toolpooler.RabbitMQPooler;
import com.blastme.messaging.toolpooler.RedisPooler;
import com.blastme.messaging.toolpooler.RouteSMSPooler;
import com.blastme.messaging.toolpooler.SMSTransactionOperationPooler;
import com.blastme.messaging.toolpooler.TelecomPrefixPooler;
import com.blastme.messaging.toolpooler.Tool;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import io.lettuce.core.api.sync.RedisCommands;

public class SMSAutoGenANumberProcessorNewPasti {
	private static Logger logger;	
	private Tool tool;
	
	//private static String queueBatchToProcess = "AUTOGENAUPLOADER_TOPUSH_BATCHID";
	//private static String queueBatchToProcess = "AUTOGENAUPLOADER_TOPUSH_BATCHID_PINTAR";
	private static String queueBatchToProcess = "AUTOGENAUPLOADER_TOPUSH_BATCHID_PASTI";
	
	//private static String queueIncomingMessage = "SMS_INCOMING";
	//private static String queueIncomingMessage = "SMS_INCOMING_PINTAR";
	private static String queueIncomingMessage = "SMS_INCOMING_PASTI";
	
	private static String smsChannel = "AUTOGENSMS";
	private String batchFileDirectory;
	private DateTimeFormatter dtfNormal = DateTimeFormatter.ofPattern("yy-MM-dd HH:mm:ss.SSS");
	
	private static RedisPooler redisPooler;
	private static RedisCommands<String, String> redisCommand;

	private Connection connection = null;
	private PreparedStatement statement = null;
	private PreparedStatement statementIsProcessed = null;
	//private PreparedStatement statementANumber = null;
	//private PreparedStatement statementANumberUpdate = null;
	private PreparedStatement statementProgress = null;
    private ResultSet resultSet = null;
    
    private SMSTransactionOperationPooler smsTransactionOperationPooler;
    private ClientPropertyPooler clientPropertyPooler;
    private RabbitMQPooler rabbitMqPooler;
    private com.rabbitmq.client.Connection connectionRabbit;
    private Channel channelRabbitPublish;
    private Channel channelRabbitConsume;

    private String[] NDCHongkong = {"906", "914", "920", "934", "951", "958", "960", "971", "979", "606", "609", "615", "622", "623", "635", "643", "645", "648", "657", "664", "667", 
                                    "670", "673", "674", "676", "684", "685", "687", "691", "693", "694", "695", "510", "512", "513", "516", "517", "531", "537", "539", "530", "534",
                                    "621", "658", "699", "544", "548", "549", "542", "562", "561", "560", "564", "598", "593", "553", "551", "557", "526", "522", "5283", "570", "574",
                                    "4648", "9290", "462", "636", "514", "518", "523", "536", "541", "558", "590", "594", "612", "618", "625", "630", "638", "644", "649", "651", "660",
                                    "662", "669", "680", "692", "907", "923", "927", "952", "964", "969", "977", "980", "511", "514", "521", "524", "527", "528", "529", "540", "543",
                                    "547", "552", "554", "568", "571", "579", "591", "592", "599", "602", "603", "605", "613", "614", "616", "617", "628", "629", "637", "639", "641",
                                    "642", "653", "654", "659", "665", "671", "675", "677", "682", "689", "697", "902", "903", "908", "909", "910", "915", "918", "919", "921", "925",
                                    "926", "930", "933", "940", "946", "953", "955", "961", "962", "965", "976", "978", "987", "988", "511", "514", "520", "521", "524", "527", "528",
                                    "529", "532", "540", "543", "547", "552", "554", "568", "570", "571", "579", "591", "592", "599", "602", "603", "605", "613", "614", "616", "617",
                                    "628", "629", "636", "637", "639", "641", "642", "653", "654", "659", "665", "671", "675", "677", "682", "689","697", "902", "903", "908", "909",
                                    "910", "915", "918", "919", "921", "923", "925", "926", "930", "933", "940", "946", "953", "955", "961", "962", "965", "976", "978", "987", "988",
                                    "4610", "4611", "4613", "4614", "4640", "4647", "5749", "6362", "8481", "6454", "4655", "4656", "4657", "4658", "7080", "7081", "7082", "7083", "7084",
                                    "5149", "6040", "6041", "6042", "6043", "6044", "607", "608", "610", "619", "620", "640", "647", "650", "679", "904", "912", "928", "932", "935",
                                    "936", "937", "941", "942", "947", "948", "949", "950", "957", "963", "967", "974", "975", "981", "983", "5289", "533", "546", "556", "563", "566", 
                                    "569", "596", "597", "6045", "6046", "6047", "6048", "6049", "631", "633", "634", "668", "690", "519", "970", "982", "6260", "601", "611", "632",
                                    "646", "901", "913", "916", "917", "922", "924", "931", "938", "943", "944", "945", "954", "966", "968", "972", "973", "984", "985", "986", "559",
                                    "550", "523", "8329", "528", "574", "572", "464", "707", "462", "848"};
   
    private String[] NDCMalaysia = {"19", "13", "148", "145", "1030", "1031", "1032", "1033", "1034", "1040", "1041", "1050", "1051", "1052", "1053", "1054", "1057", "1058", "1059",
    		"1077", "1078", "1079", "1080", "1081", "1083", "1084", "1085", "1086", "1087", "11105", "11106", "11107", "11108", "11109", "11130", "11131", "11132", "11133", "11134",
    		"11145", "11146", "11147", "11148", "11149", "1115", "11185", "11186", "11187", "11188", "11189", "1119", "11205", "11206", "11207", "11208", "11209", "1129", "11320",
    		"11321", "11322", "11323", "11324", "11325", "11326", "11327", "11329", "1135", "1140", "1141", "1152", "1592", "14400", "11245", "11246", "11247", "11248", "11249",
    		"11560", "11561", "11562", "11563", "11564", "1154", "1155", "11530", "11535", "11565", "11575", "11585", "11595", "11566", "11587", "11589", "11577", "11567", "11568",
    		"11569", "11576", "11578", "11579", "11586", "11588", "11596", "11597", "11598", "11599", "11531", "11532", "11533", "11534", "11536", "11537", "11538", "11539", "11328", "16",
    		"102", "143", "146", "149", "1036", "1037", "1038", "1039", "1046", "1056", "1066", "1076", "1082", "1088", "1091", "1092", "1093", "1094", "1095", "1096", "1097", "1098",
    		"1116", "1122", "1126", "1131", "1133", "1136", "1150", "1151", "1591", "10900", "10901", "10902", "10903", "10905", "10906", "10907", "10908", "10909", "11200", "11201",
    		"11202", "11203", "11204", "11590", "11646", "11647", "11648", "11692", "11703", "11704", "11705", "11706", "11707", "11708", "11709", "11710", "11711", "15960", "15961",
    		"15962", "15963", "15964"};

    private String[] NDCIndonesia = {"814", "815", "816", "855", "856", "857", "858", "814", "815", "816", "855", "856", "857", "858", "814", "815", "816", "855", "856", "857", "858",
    		"817", "818", "819", "859", "878", "877", "831", "838", "833", "832", "811", "812", "813", "852", "853", "821", "822", "823", "851", "881", "882", "888", "889", "895", "896",
    		"897", "898", "899"};
    
    private String[] NDCPhilippines = {"905", "906", "915", "916" , "917", "926", "927", "935", "936", "937", "945", "955", "956", "965", "966", "967", "975", "976", "977", "995", "996", 
    		"997", "952", "953", "954", "957", "959", "971", "972", "978", "922", "923", "925", "932", "933", "942", "943", "813", "907", "908", "909", "910", "912", "918", "919", "920",
    		"921", "922", "923", "925", "928", "929", "930", "931", "932", "933", "938", "939", "942", "943", "946", "947", "948", "949", "950", "962", "981", "998", "999", "951", "960",
    		"961", "968", "958", "982", "963"};
    
    private String[] NDCSingapore = {"812", "826", "828", "830", "831", "842", "843", "845", "873", "901", "903", "905", "911", "912", "913", "915", "917", "923", "935", "937", "939",
    		"944", "946", "971", "972", "973", "975", "977", "978", "981", "986", "989", "891"};
    
    private String[] NDCLaos = {"202", "302", "2050", "2051", "2052", "2053", "2054", "2055", "2056", "2057", "2058", "2059", "305", "209", "309", "304"};
    
	public SMSAutoGenANumberProcessorNewPasti() {
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Setup logger
		logger = LogManager.getLogger("AUTOGENANUMBER_PROCESSOR");

		// Initiate LoggingPooler
		new LoggingPooler();
		
		// Initiate tool
		tool = new Tool();
		
		// Initiate batchFileDirectory
		//batchFileDirectory = Configuration.getUploadBatchFileDirectory();
		//batchFileDirectory = "/app/web_pintar/uploadedBNumber/";
		batchFileDirectory = "/app/web_pasti/uploadedBNumber/";
		
		// Initiate smsTransactionOperationPooler
		smsTransactionOperationPooler = new SMSTransactionOperationPooler();
		
		// Initiate TelecomPrefixPooler
		new TelecomPrefixPooler();
		
		// Initiate RouteSMSPooler
		new RouteSMSPooler();
		
		// clientPropertyPooler
		clientPropertyPooler = new ClientPropertyPooler();
		
        try {
    		// Initiate RedisPooler
    		redisPooler = new RedisPooler();
    		redisCommand = redisPooler.redisInitiateConnection();
    		
    		// Initiate RabbitMQPooler
    		rabbitMqPooler = new RabbitMQPooler();	
    		connectionRabbit = rabbitMqPooler.getConnection();
    		
    		// Channel for publish
    		channelRabbitPublish = rabbitMqPooler.getChannel(connectionRabbit);
			channelRabbitPublish.queueDeclare(queueIncomingMessage, true, false, false, null);
			
			// Channel for reading
			channelRabbitConsume = rabbitMqPooler.getChannel(connectionRabbit);
			channelRabbitConsume.queueDeclare(queueBatchToProcess, true, false, false, null);

    		// Initiate connection to Postgresql
            BasicDataSource bds = DataSource.getInstance().getBds();
            connection = bds.getConnection();
    		LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "SMSAutoGenANumberProcessor", false, false, false, "", 
    				"Application AutoGeneratedANumberProcessor is loaded and run.", null);
    		
    		// Select job
			statement = connection.prepareStatement("select username, no_of_records, no_of_sms_each, message, message_encoding, b_number_filename, "
					+ "message_length, no_of_sms_total, country_code from autogen_a_number where is_processed = false and batch_id = ?");

    		// Get autogen_a_number as batchId
			statementIsProcessed = connection
					.prepareStatement("update autogen_a_number set is_processed = true where batch_id = ?");

			// Get allocated a number
			// statementANumber = connection.prepareStatement("select a_number from a_number where (last_usage_date < ? or last_usage_date is null) and country_code = ? limit ?");
			
			// Updating last_usage_date of a number
			//statementANumberUpdate = connection.prepareStatement("update a_number set last_usage_date = ? where a_number = ?");

			// Insert into table autogenbnumberprogress
			statementProgress = connection.prepareStatement("INSERT INTO autogenbnumberprogress"
					+ "(batch_id, sms_count, sent_count, last_update_datetime, client_id) VALUES (?, ?, ?, ?, ?)");
        } catch(Exception e) {
        	e.printStackTrace();
    		LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "SMSAutoGenANumberProcessor", true, false, false, "", 
    				"Failed to load connection to database server. Error occured.", e);
    		
    		System.exit(-1);
        }

	}
	
	private JSONObject isPrefixValid(String msisdn){
		JSONObject jsonPrefix = new JSONObject();
		jsonPrefix.put("isValid", false);
		
		Iterator<String> keys = TelecomPrefixPooler.jsonPrefixProperty.keys();
		System.out.println("jsonPrefixProperty: " + TelecomPrefixPooler.jsonPrefixProperty.toString());
		
		while(keys.hasNext()){
			String key = keys.next();
			if(msisdn.startsWith(key)){
				jsonPrefix.put("isValid", true);
				jsonPrefix.put("prefix", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("prefix"));
				jsonPrefix.put("telecomId", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("telecomId"));
				jsonPrefix.put("countryCode", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("countryCode"));
				
				break;
			}
		}
		
		return jsonPrefix;
	}
	
	private int getGSM7MessageLength(String message){
		int length = 300;
		
		int count = 0;
		for(int x = 0; x < message.length(); x++){
			String theChar = message.substring(x, x + 1); 
			
			if(theChar.equals("[")){
				count = count + 2;
			} else if(theChar.equals("]")){
				count = count + 2;
			} else if(theChar.equals("^")){
				count = count + 2;
			} else if(theChar.equals("~")){
				count = count + 2;
			} else if(theChar.equals("{")){
				count = count + 2;
			} else if(theChar.equals("}")){
				count = count + 2;
			} else if(theChar.equals("€")){
				count = count + 2;
			} else if(theChar.equals("/")){
				count = count + 2;
			} else if(theChar.equals("\\")){
				count = count + 2;
			} else {
				count = count + 1;
			}
		}
		
		if(count > 0){
			length = count;
		}
		
		return length;
	}
	
	private int getSmsCount(String message, String encoding){
		if(encoding.trim().equals("GSM7")){
			if(getGSM7MessageLength(message) <= 160){
				return 1;
			} else {
				return (int) Math.ceil(getGSM7MessageLength(message) / 153.00);
			}
		} else if(encoding.trim().equals("UCS2")) {
			if(message.length() <= 70){
				return 1;
			} else {
				return (int) Math.ceil(message.length() / 67.00);
			}
		} else {
			return (int) Math.ceil(message.length() / 67.00);
		}
	}
	
	private void updateIsProcessed(String batchId){
		try{			
			statementIsProcessed.setString(1, batchId);
			statementIsProcessed.executeUpdate();
			
			LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "updateIsProcessed", false, false, false,"", 
					"Succesfully update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", null);	
		} catch (Exception e) {
			e.printStackTrace();
			
			LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "updateIsProcessed", true, false, false,"", 
					"Failed to update batchId " + batchId + " ALREADY PROCESSED TO TRUE.", e);				
		}
	}
	
	private boolean isGSM7(String theText){
		boolean isGSM = false;
		
		String theRegex = "^[A-Za-z0-9 \\r\\n@£$¥èéùìòÇØøÅå\u0394_\u03A6\u0393\u039B\u03A9\u03A0\u03A8\u03A3\u0398\u039EÆæßÉ!\"#$%&'()*+,\\-./:;<=>?¡ÄÖÑÜ§¿äöñüà^{}\\\\\\[~\\]|\u20AC]*$";
		Pattern pattern = Pattern.compile(theRegex);
		
		Matcher matcher = pattern.matcher(theText);
		
		isGSM = matcher.matches();
		
		return isGSM;
	}
	
	private String[] getAllocatedANumbers(String countryCode, int recordCount){
		String[] arrANumber = new String[recordCount];
		
		// Random index
		Random rand = new Random();
		
		try{
//			LocalDate nowDate = LocalDate.now();
//			statementANumber.setObject(1, nowDate);
//			statementANumber.setString(2, countryCode);
//			statementANumber.setInt(3, recordCount);
//			System.out.println("statementANumber: " + statementANumber.toString());
//			
//			ResultSet resultSetANumber = statementANumber.executeQuery();
//			
//			int countANumber = 0;
//			while(resultSetANumber.next()){
//				arrANumber[countANumber] = resultSetANumber.getString("a_number");
//				System.out.println("Load aNumber: " + arrANumber[countANumber]);
//				
//				if(arrANumber[countANumber] == null){
//					break;
//				}
//				
//				countANumber = countANumber + 1;
//			}
//			System.out.println("countANumber: " + countANumber);
			
			
			// New system update, all ANumber is autogenerated
//			for (int x = 0; x < recordCount; x++) {
//				String aNumber = tool.generateUniqueId("NUMERIC", 13).trim();
//				
//				if (aNumber.startsWith("0")) {
//					aNumber = aNumber.replaceFirst("0", "8");
//				}
//				
//				// Add prefix countryCode
//				aNumber = countryCode + aNumber.trim();
//				
//				arrANumber[x] = aNumber;
//				
//				System.out.println("x: " + x + ". arrNumber[" + x + "]: " + aNumber);
//			}
			
			// Newer system update - a number goes with certain format, only for Indonesia, malaysia and hongkong
			for (int x = 0; x < recordCount; x++) {
				String theANumber = "";
				if (countryCode.trim().equals("852")) {
					int upperBound = NDCHongkong.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Hongkong Index: " + Integer.toString(selectedIndex), null);	
					
					String hongkongPrefix = "677";
					if (selectedIndex < NDCHongkong.length) {
						hongkongPrefix = NDCHongkong[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Hongkong Prefix: " + hongkongPrefix, null);	
					
					theANumber = countryCode + hongkongPrefix.trim() + tool.generateUniqueId("NUMERIC", 4).trim();
				} else if (countryCode.trim().equals("60")) {
					int upperBound = NDCMalaysia.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Malaysia Index: " + Integer.toString(selectedIndex), null);	
					
					String malaysiaPrefix = "1046";
					if (selectedIndex < NDCMalaysia.length) {
						malaysiaPrefix = NDCMalaysia[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Malaysia Prefix: " + malaysiaPrefix, null);	
					
					theANumber = countryCode + malaysiaPrefix.trim() + tool.generateUniqueId("NUMERIC", 6).trim();
				} else if (countryCode.trim().equals("63")) {
					int upperBound = NDCPhilippines.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Phillippines Index: " + Integer.toString(selectedIndex), null);	
					
					String philippinesPrefix = "919"; // Default
					if (selectedIndex < NDCPhilippines.length) {
						philippinesPrefix = NDCPhilippines[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Philipines Prefix: " + philippinesPrefix, null);	
					
					theANumber = countryCode + philippinesPrefix.trim() + tool.generateUniqueId("NUMERIC", 7).trim();
				} else if (countryCode.trim().equals("65")) {
					// Default is Indonesia
					countryCode = "65";

					int upperBound = NDCSingapore.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Singapore Index: " + Integer.toString(selectedIndex), null);	
					
					String singaporePrefix = "831";
					if (selectedIndex < NDCSingapore.length) {
						singaporePrefix = NDCSingapore[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Singapore Prefix: " + singaporePrefix, null);	
					
					theANumber = countryCode + singaporePrefix.trim() + tool.generateUniqueId("NUMERIC", 5).trim();
				} else if (countryCode.trim().equals("856")) {
					countryCode = "856";
					
					int upperBound = NDCLaos.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Laos Index: " + Integer.toString(selectedIndex), null);	
					
					String laosPrefix = "302";
					if (selectedIndex < NDCLaos.length) {
						laosPrefix = NDCLaos[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Laos Prefix: " + laosPrefix, null);	
					
					// Panjang sisa
					int maxLength = 12;
					if (laosPrefix.startsWith("205") || laosPrefix.startsWith("202")) {
						maxLength = 13;
					}
					
					int sisaLength = maxLength - (countryCode.length() + laosPrefix.trim().length());
					
					if (sisaLength <= 0) {
						sisaLength = 6;
					}
					
					theANumber = countryCode + laosPrefix.trim() + tool.generateUniqueId("NUMERIC", sisaLength).trim();					
				} else {
					// Default is Indonesia
					countryCode = "62";

					int upperBound = NDCIndonesia.length;
					
					int selectedIndex = rand.nextInt(upperBound);
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"upperBound: " + Integer.toString(upperBound) + ", Selected Indonesia Index: " + Integer.toString(selectedIndex), null);	
					
					String indonesiaPrefix = "811";
					if (selectedIndex < NDCIndonesia.length) {
						indonesiaPrefix = NDCIndonesia[selectedIndex];
					}
					
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", false, false, false,"", 
							"Selected Indonesia Prefix: " + indonesiaPrefix, null);	
					
					theANumber = countryCode + indonesiaPrefix.trim() + tool.generateUniqueId("NUMERIC", 8).trim();
				}
				
				arrANumber[x] = theANumber;
			}			
		} catch (Exception e) {
			e.printStackTrace();
			LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "getAllocatedANumbers", true, false, false,"", 
					"Failed to read a number. Error occured.", e);	
		}
		
		return arrANumber;
	}
	
	private void updateANumberLastUsage(String aNumber){
		try{
//			LocalDate nowDate = LocalDate.now();
//			
//			statementANumberUpdate.setObject(1, nowDate);
//			statementANumberUpdate.setString(2, aNumber);
//			
//			statementANumberUpdate.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();			
			LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "doUpdateANumber", true, false, false,"", 
					"Failed update a number for last usage of aNumber: " + aNumber + ". Error occured.", e);	
		}
	}
	
	private int doProcessTheBatch(String clientId, String batchId, String theSMS, String messageEncoding, String username, String fileName, 
			String aNumberCountryCode, int recordCount, String queueMessage){
		int result = 0;
		
		// Initiate formatter
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

		// Start Date Time
		LocalDateTime startDateTime = LocalDateTime.now();

		try{
			// Get allocated a number
			String[] arrANumber = getAllocatedANumbers(aNumberCountryCode, recordCount);
			
			// Open file for the batch
			String completeFileName = batchFileDirectory + fileName.trim();
			
			BufferedReader br = new BufferedReader(new FileReader(completeFileName));
			String line = "";
			int count = 0;
			
			while((line = br.readLine()) != null){
				// Clean up space, dash, +, ( and )
				String bNumber = line.trim();
				bNumber = bNumber.replace(" ", "");
				bNumber = bNumber.replace("-", "");
				bNumber = bNumber.replace("+", "");
				bNumber = bNumber.replace("(", "");
				bNumber = bNumber.replace(")", "");
				
				System.out.println("Line " + count + ": " + bNumber);
				
				// Map bNumber with aNumber
				LocalDateTime now = LocalDateTime.now();				
				String aNumber = arrANumber[count];
				
				if(aNumber == null){
					// Not processed
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "doProcessTheBatch", false, false, false,"", 
							"Strop procesing batchId " + batchId + ". No aNumber found.", null);	
				} else {
					System.out.println(now.format(dtfNormal) + ". bNumber: " + bNumber + " will use aNumber: " + aNumber);
					
					// Update lastusage aNumber
					updateANumberLastUsage(aNumber);
					
					// Set messageId
					String messageId = tool.generateUniqueID();
				
					// Set errorCode
					String errorCode = "003"; // MESSAGE IS BEING PROCESSED
					
					// Reconfirm messageEncoding
					boolean isGSM7 = isGSM7(theSMS);
//					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
//							"isGSM7: " + isGSM7, null);
					if(isGSM7 == false){
						messageEncoding = "UCS2";
					}
					
					// Set clientSenderId
					String dbClientSenderId = "*AUTOGEN*";
					String clientSenderId = dbClientSenderId + "-" + aNumber;
					
					// Set clientSenderIdId
					String clientSenderIdId = clientSenderId + "-" + clientId;
					
	            	// Check countryCode and Prefix
	            	JSONObject jsonPrefix = isPrefixValid(bNumber);
	            	
	            	String countryCode = "";
	            	String prefix = "";
	            	String telecomId = "";
	            	if(jsonPrefix.getBoolean("isValid") == true){
	            		countryCode = jsonPrefix.getString("countryCode");
	            		prefix = jsonPrefix.getString("prefix");
	            		telecomId = jsonPrefix.getString("telecomId");
	            	}
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"countryCode: " + countryCode + ", prefix: " + prefix + ", telecomId: " + telecomId, null);	

	            	// Get clientPrice and clientCurrency
					String routeId =  dbClientSenderId + "-" + clientId + "-" + username + "-" + telecomId;
					LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
							"routeId: " + routeId, null);
					
					if(RouteSMSPooler.jsonRouteSMSProperty.has(routeId)){
						LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"jsonRouteSMSProperty has routeId: " + routeId, null);	
	
						JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);
						
						// Get client price from jsonRoute
						double clientPricePerSubmit = 0.00000;
						String clientCurrency = "IDR";
	
						clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");
						clientCurrency = clientPropertyPooler.getCurrencyId(clientId.trim());
		            	
						System.out.println("MessageEncoding: " + messageEncoding + ", Message length: " + theSMS.length() + ", SMS Count: " + getSmsCount(theSMS, messageEncoding));
						
		            	// Save initial data before send to router
						smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "AUTOGENAUPLOADER: " + queueMessage, "", 
								"", now, now, bNumber, theSMS, countryCode, prefix, telecomId, errorCode, 
								"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
								username, clientPricePerSubmit, clientCurrency, messageEncoding, getGSM7MessageLength(theSMS), 
								getSmsCount(theSMS, messageEncoding));
						
//						LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
//								"Successfully save Initial Data to Database.", null);	
						
						// Submit to Redis as initial data with expiry 7 days
						int expiry = 7 * 24 * 60 * 60;
						JSONObject jsonRedis = new JSONObject();
						jsonRedis.put("messageId", messageId);
						jsonRedis.put("receiverDateTime", now.format(formatter));
						jsonRedis.put("transactionDateTime", now.format(formatter));
						jsonRedis.put("msisdn", bNumber);
						jsonRedis.put("message", theSMS);
						jsonRedis.put("telecomId", telecomId);
						jsonRedis.put("countryCode", countryCode);
						jsonRedis.put("prefix", prefix);
						jsonRedis.put("errorCode", errorCode);
						jsonRedis.put("apiUserName", username);
						jsonRedis.put("clientSenderIdId", clientSenderIdId);
						jsonRedis.put("clientSenderId", clientSenderId);
						jsonRedis.put("clientId", clientId);
						jsonRedis.put("clientIpAddress", "");
						jsonRedis.put("receiverType", smsChannel); // AUTOGENAUPLOADER, SMPP and HTTP only
	
						String redisKey = "trxdata-" + messageId.trim();
						String redisVal = jsonRedis.toString();
						
						redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
						LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"Successfully save Initial Data to Database and Redis.", null);
	
		            	// Send to INCOMING MESSAGE queue
		            	JSONObject jsonIncoming = new JSONObject();
		            	
		            	jsonIncoming.put("messageId", messageId);
						jsonIncoming.put("apiUserName", username);
		            	jsonIncoming.put("msisdn", bNumber);
		            	jsonIncoming.put("message", theSMS);
		            	jsonIncoming.put("clientId", clientId);
		            	jsonIncoming.put("telecomId", telecomId);
		            	jsonIncoming.put("clientSenderId", clientSenderId);
		            	jsonIncoming.put("smsChannel",smsChannel);
		            	jsonIncoming.put("messageLength", getGSM7MessageLength(theSMS));
		            	jsonIncoming.put("messageCount", getSmsCount(theSMS, messageEncoding));
		            	jsonIncoming.put("clientPricePerSubmit", String.format("%.5f", clientPricePerSubmit));
		            	
		            	// Publish jsonIncoming to queue INCOMING_SMS
						channelRabbitPublish.basicPublish("", queueIncomingMessage, MessageProperties.PERSISTENT_TEXT_PLAIN, jsonIncoming.toString().getBytes());
						LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"jsonIncoming: " + jsonIncoming.toString() + " published SUCCESSfully!", null);
					} else {
						LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false, messageId, 
								"Route is NOT DEFINED for batchId " + batchId + ", routeId " + routeId, null);
						
						//Save initial db with status failed
						errorCode = "112";
						smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, batchId, "AUTOGENAUPLOADER: " + queueMessage, "", 
								"", now, now, bNumber, theSMS, countryCode, prefix, telecomId, errorCode, 
								"AUTOGENAUPLOADER", clientSenderIdId, clientSenderId, clientId, 
								username, 0.00, "", messageEncoding, getGSM7MessageLength(theSMS), 
								getSmsCount(theSMS, messageEncoding));
					}					
				}
				
				
				count = count + 1;
			}
			
			// Save progress counter
			// batch_id, sms_count, sent_count, last_update_datetime
			LocalDateTime now = LocalDateTime.now();				

			statementProgress.setString(1, batchId);
			statementProgress.setInt(2, getSmsCount(theSMS, messageEncoding) * count);
			statementProgress.setInt(3, 0);
			statementProgress.setObject(4, now);
			statementProgress.setString(5, clientId);
			
			statementProgress.executeUpdate();


			br.close();
			
			// End Date Time
			LocalDateTime endDateTime = LocalDateTime.now();
			
			// Calculate speed (transactions per second)
			long durationMilliSecond = Duration.between(startDateTime, endDateTime).toMillis();
			System.out.println("Traffic: " + Integer.toString(count) + ". Duration: " + String.valueOf(durationMilliSecond) + " milliseconds. TPS: " + String.valueOf((count*1000.000000000000000)/durationMilliSecond));
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return result;
	}
	
	private void processQueueBatch(String queueMessage){
		try{			
			// Get batchId and clientId from queueMessage
			JSONObject jsonMessage = new JSONObject(queueMessage);
			
			String batchId = jsonMessage.getString("batchId");
			String clientId = jsonMessage.getString("clientId");
			String username = jsonMessage.getString("username");
			
			LoggingPooler.doLog(logger, "DEBUG", "SMSAutoGenANumberProcessor", "processQueueBatch", false, false, false,"", 
					"Incoming data - batchId: " + batchId + ", clientId: " + clientId + ", username: " + username, null);	

			// Query table autogenbnumberuploaderdetail, get the batch which alerady_processed == false or null
			try{
				statement.setString(1, batchId);
				
				resultSet = statement.executeQuery();
				
				while(resultSet.next()){	
	            	// Get now localdatetime
					String fileName = resultSet.getString("b_number_filename");
					String theSMS = resultSet.getString("message");
					String aNumberCountryCode = resultSet.getString("country_code");
					int recordCount = resultSet.getInt("no_of_records");
					String messageEncoding = resultSet.getString("message_encoding");
					
					// Update status is_processed = true
					updateIsProcessed(batchId);
					
					// Process the batch
					doProcessTheBatch(clientId, batchId, theSMS, messageEncoding, username, fileName, aNumberCountryCode, recordCount, queueMessage);
				}				
			} catch (Exception e) {
				e.printStackTrace();
	    		LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "processQueueBatch", true, false, false, "", 
	    				"Failed to process incoming instruction. Error occured.", e);
			} 
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void readQueue(){
		// Listen to queue that contains information about which batchnumber to process - queuename: AUTOGEN_BATCH_INSTRUCT
		// Incoming message will be: {"batchId": "1928394830292", "clientId": "1029384"}
		try{
			LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "readQueue", false, false, true, "", 
					"Reading queue is ready, waiting for message ... ", null);
			
			Consumer consumer = new DefaultConsumer(channelRabbitConsume) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        
					try{
						LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "readQueue", false, false, true, "", 
								"Receive message: " + message, null);

						// Process the message from queue
				        processQueueBatch(message);
					} finally {
						LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "readQueue", false, false, true, "", 
								"Done processing message: " + message, null);

						channelRabbitConsume.basicAck(envelope.getDeliveryTag(), false);
					}
			      }
			};

			boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
			channelRabbitConsume.basicQos(1); // Read one by one, so if other app SMSAutoGenANumberProcessor running, it can share load
			channelRabbitConsume.basicConsume(queueBatchToProcess, autoAck, consumer);			
		} catch (Exception e){
			LoggingPooler.doLog(logger, "INFO", "SMSAutoGenANumberProcessor", "readQueue", true, false, false, "", 
					"Failed to access queue " + queueBatchToProcess, e);
		}
	}

	public static void main(String[] args) {
		System.out.println("Starting SMSAutoGenANumberProcessor ...");
		
		SMSAutoGenANumberProcessorNewPasti processor = new SMSAutoGenANumberProcessorNewPasti();
		processor.readQueue();
	}

}
