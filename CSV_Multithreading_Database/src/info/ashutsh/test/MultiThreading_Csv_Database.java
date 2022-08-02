package info.ashutsh.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import info.ashutsh.model.CSV_Data;
import info.ashutsh.service.CSVReaderService;
import info.ashutsh.service.PersistenceService;

public class MultiThreading_Csv_Database {

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, SQLException {
		// Start Time
		long startTime = System.nanoTime();

		CSVReaderService csvReaderService = new CSVReaderService();
		// Read raw Data From CSV File
		// This is single threaded as disk I/O would be catering to one request at
		// a time and multiple threads will not improve performance
		List<CSV_Data> rawData = csvReaderService.readCSV(
				System.getProperty("user.dir") + "\\src\\info\\ashutsh\\configuration\\100000 Sales Records.csv");


		// partitioning the List<CSV_Data> in
		int partitionSize = 10000;
		List<List<CSV_Data>> partitions = new LinkedList<List<CSV_Data>>();
		
		for (int i = 0; i < rawData.size(); i += partitionSize) {
			partitions.add(rawData.subList(i, Math.min(i + partitionSize, rawData.size())));
		}

		// process raw data with multiple thread to get Java object that represents a
		// single data record// process in parallel using multiple threads

		// Each Thread is assign to do Bulk Persistence Operation which inserts record in database in batches
		partitions.stream().parallel().map(singleLine -> new PersistenceService().loadDataToDB(singleLine))
				.collect(Collectors.toList());

		// PersistenceService.loadDataToDB(rawData);

		// End Time (Time Consumption)
		long endTime = System.nanoTime();
		long finalTimeElapsed = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime);
		System.out.println("Execution time in seconds: ~ " + finalTimeElapsed);

	}

}
