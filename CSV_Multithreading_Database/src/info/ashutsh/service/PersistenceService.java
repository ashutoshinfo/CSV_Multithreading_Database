package info.ashutsh.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import info.ashutsh.model.CSV_Data;
import info.ashutsh.util.DatabaseConnection;

/**
 * Service that helps persisting data from CSV
 * 
 * @author Ashutosh Patel
 *
 */

public class PersistenceService {

	Connection connection = null;
	PreparedStatement statement = null;

	public boolean loadDataToDB(List<CSV_Data> dbrList) {
		int count = 0;// counts the numbers of data
		int batchSize = 10000;// Fix Batch Size

		try {
			// Getting Connection
			// TODO: Use DBCP connection pool
			connection = DatabaseConnection.getConnection();

			// Setting Auto Commit False in case of any exception and do manual commit;
			connection.setAutoCommit(false);
			// Creating SQL Query
			String sql = "INSERT INTO csv_data (name, age, salary) VALUES (?, ?, ?)";

			// Passing SQL Query to PrepareStatement
			statement = connection.prepareStatement(sql);

			// Getting Each Properties from CSV_Data Object and setting ParameterIndex
			for (CSV_Data csv_Data : dbrList) {

				try {
					statement.setString(1, csv_Data.getName());
					statement.setInt(2, csv_Data.getAge());
					statement.setDouble(3, csv_Data.getSalary());
					statement.addBatch();
				} catch (Exception e) {
					System.err.println("Invalid Value Exception !");
				}

				count++;

				// Tracks The Batch size - Once reaches to batchSize(int) it will Execute Batch
				if (count % batchSize == 0) {
					System.out.println("Thread Name: " + Thread.currentThread().getName() + " | Batch Count: " + count);
					// Execute Batch
					statement.executeBatch();

				}
			}

			statement.executeBatch(); // Execute the Remaining Queries
			connection.commit(); // Committing all record
			return true;

		} catch (SQLException ex) {
			ex.printStackTrace();
			try {
				// Rollback if any Exception Rise
				connection.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} // Closing Connection
		}

	}

}
