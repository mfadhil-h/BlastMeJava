package com.blastme.messaging.dummy;

import java.io.File;
import java.io.FileOutputStream;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.DefaultTempFileCreationStrategy;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import com.blastme.messaging.configuration.Configuration;

public class XLSXAttachmentIndexPopulator {

	private void generateData(){
		String fileName = "index.xlsx";
		
		// Load Configuration
		new Configuration();
		LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
		File file = new File(Configuration.getLogConfigPath());
		context.setConfigLocation(file.toURI());
		
		// Set timezone
		TimeZone.setDefault(TimeZone.getTimeZone(Configuration.getTimeZone()));

		try{
			File dir = new File(Configuration.getEmailAttachmentIndexPath() + "temp");
			dir.mkdir();
			TempFile.setTempFileCreationStrategy(new DefaultTempFileCreationStrategy(dir));

			Workbook wb = new SXSSFWorkbook();
			//New Sheet
	        SXSSFSheet sheet1 = (SXSSFSheet) wb.createSheet("INDEX");
	        
			Font headerFontBoldNormal = wb.createFont();
			headerFontBoldNormal.setBold(true);
			headerFontBoldNormal.setFontHeightInPoints((short) 16);
			headerFontBoldNormal.setColor(IndexedColors.BLACK.getIndex());
	        
			CellStyle headerFontBoldNormalStyle = wb.createCellStyle();
			headerFontBoldNormalStyle.setFont(headerFontBoldNormal);
			headerFontBoldNormalStyle.setAlignment(HorizontalAlignment.CENTER);
			headerFontBoldNormalStyle.setBorderTop(BorderStyle.MEDIUM);
			headerFontBoldNormalStyle.setBorderRight(BorderStyle.MEDIUM);
			headerFontBoldNormalStyle.setBorderBottom(BorderStyle.MEDIUM);
			headerFontBoldNormalStyle.setBorderLeft(BorderStyle.MEDIUM);
			headerFontBoldNormalStyle.setFillBackgroundColor(IndexedColors.AQUA.getIndex());
			headerFontBoldNormalStyle.setFillPattern(FillPatternType.LEAST_DOTS);

	        int rowTitle = 0;
	        sheet1.trackAllColumnsForAutoSizing();
	        // Create title rows
			Row row = sheet1.createRow(rowTitle);

			String[] judul = new String[]{"TO EMAIL ADDRESS", "SENDER ID", "SUBJECT", "MESSAGE", "ATTACHMENT FILE NAME 01", 
					"ATTACHMENT FILE NAME 02", "ATTACHMENT FILE NAME 03", "ATTACHMENT FILE NAME 04", "ATTACHMENT FILE NAME 05"};
			
			for(int x = 0; x < judul.length; x++){
				Cell cellContent = row.createCell(x);
				cellContent.setCellValue(judul[x]);
				cellContent.setCellStyle(headerFontBoldNormalStyle);
				
				sheet1.autoSizeColumn(x);

				System.out.println("Processing column " + judul[x]);
			}
						
			// Sampe
			row = sheet1.createRow(rowTitle + 1);
			
			String[] sample = new String[]{"sample@contoh.com", "C0nt0h01", "Subject of the example.", "<H1>Hi there</H1>,<p>This is just content of the example.</p>", "invoice.pdf", 
					"usage.xlsx", "logo.jpg", "", ""};
			
			for(int y = 0; y < judul.length; y++){
				Cell cellContent = row.createCell(y);
				cellContent.setCellValue(sample[y]);
				
				System.out.println("Processing column " + sample[y]);
			}
			
			// Flush data to file
			FileOutputStream outputStream = new FileOutputStream(Configuration.getEmailAttachmentIndexPath() + fileName);
			wb.write(outputStream);
			wb.close();
			outputStream.close();

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		XLSXAttachmentIndexPopulator excel = new XLSXAttachmentIndexPopulator();
		
		excel.generateData();
	}

}
