package info.ashutsh.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;

public class DatabaseConnection {

	private static BasicDataSource dataSource;
	private static Connection con = null;

	static {

		BasicDataSource ds = new BasicDataSource();

		FileInputStream fileInputStram = null;
		try {
			fileInputStram = new FileInputStream(
					System.getProperty("user.dir") + "\\src\\info\\ashutsh\\configuration\\properties.properties");
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
		Properties p = new Properties();
		try {
			p.load(fileInputStram);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		ds.setUrl((String) p.get("databaseURL"));
		ds.setUsername((String) p.get("username"));
		ds.setPassword((String) p.get("password"));

		ds.setMinIdle(5);
		ds.setMaxIdle(10);
		ds.setMaxOpenPreparedStatements(100);

		dataSource = ds;

	}

	public static BasicDataSource getDataSource() {

		return dataSource;
	}

	public static Connection getConnection() {
		try {
			con = dataSource.getConnection();
			return con;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return con;
	}
}
