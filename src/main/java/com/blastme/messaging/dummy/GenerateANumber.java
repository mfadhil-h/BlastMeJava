package com.blastme.messaging.dummy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

import org.apache.commons.dbcp2.BasicDataSource;

import com.blastme.messaging.toolpooler.DataSource;
import com.blastme.messaging.toolpooler.Tool;

public class GenerateANumber {

	private void generateFile(int recordCount, int isToDB){
		Tool tool = new Tool();
		Random rand = new Random();
		File file = new File("dummyANumber.txt");
		
		try {
			if(isToDB == 0){
				if(file.createNewFile()){
					System.out.println("File is created");
					System.out.println("Record count: " + recordCount);
					
					FileWriter writer = new FileWriter(file);
					for(int x = 0; x < recordCount; x++){
						int theNumber = rand.nextInt(99999999);
						String msisdn = "62811" + tool.convertIntToDigitString(8, theNumber);
						System.out.println("Write MSISDN: " + msisdn);
						writer.write(msisdn + "\n");
					}
					writer.close();
				} else {
					System.out.println("File is already created");
					System.exit(1);
				}
			} else {
		        try {
		            BasicDataSource bds = DataSource.getInstance().getBds();
		            Connection connection = bds.getConnection();
		            
					PreparedStatement statement = connection
							.prepareStatement("insert into a_number(a_number, country_code) values (?, ?)");
					
					//String[] prefixes = {"62", "65", "86", "60", "852"};
					String[] prefixes = {"66", "95", "84", "63", "855", "856"};
					
					int counter = 0;
					
					while (counter < recordCount){
						int theNumber = rand.nextInt(99999999);
						String strNumber = Integer.toString(theNumber);
						
						if(!(strNumber.startsWith("81") || strNumber.startsWith("82") || strNumber.startsWith("85") || strNumber.startsWith("87"))){
							// proceed
							for(int x = 0; x < prefixes.length; x++){
								String finalNumber = prefixes[x].trim() + strNumber.trim();
								
								try{
									statement.setString(1, finalNumber);
									statement.setString(2, prefixes[x]);
									int ha = statement.executeUpdate();
								} catch(Exception e) {
									e.printStackTrace();
								}
								
								System.out.println(counter + ". MSISDN: " + finalNumber);
								
								counter = counter + 1;
							}
						}						
					}
					statement.close();
		        } catch(Exception ex) {
		        	ex.printStackTrace();
		        } 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		if(args.length > 1){
			int counter = Integer.parseInt(args[0]);
			int isToDB = Integer.parseInt(args[1]);
			
			GenerateANumber generator = new GenerateANumber();
			generator.generateFile(counter, isToDB);
		} else {
			System.out.println("Execute: GenerateANumber [counter] [isToDB]");
			System.exit(0);
		}
	}

}
