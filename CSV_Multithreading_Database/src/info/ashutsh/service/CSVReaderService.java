package info.ashutsh.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import info.ashutsh.model.CSV_Data;

/**
 * Reader service for CSV files
 * 
 * @author Ashutosh Patel
 *
 */

public class CSVReaderService {

	public List<CSV_Data> readCSV(String pathToCSV) throws IOException {
		// Single threaded
		List<CSV_Data> row_Csv_Line = new ArrayList<>();

		// Reading csv file From given path
		BufferedReader lineReader = new BufferedReader(new FileReader(pathToCSV));
		String lineText = null;

		// Skip 1st Header Line
		lineReader.readLine();
		// read Each line and Adding into 'List<String> row_Csv_Line'
		while ((lineText = lineReader.readLine()) != null) {
			row_Csv_Line.add(processRowData(lineText));
		}

		lineReader.close();
		return row_Csv_Line;

	}

	private CSV_Data processRowData(String cSVRowLine) {
		try {
			// Spiting words(data) by ','
			String[] data = cSVRowLine.split(",");

			// Set Name
			String name = data[0];

			// Set Age by Converting String date to age
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
			LocalDate localDate = LocalDate.parse(data[1], formatter);
			int age = Period.between(localDate, LocalDate.now()).getYears();

			// Set Salary
			Double salary = Double.parseDouble(data[2]);
			return new CSV_Data(name, age, salary);

		} catch (Exception e) {
			System.err.println("Durty Line Parsing Exception !");
			return null;
		}

	}

}
