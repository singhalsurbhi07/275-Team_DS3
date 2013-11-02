package poke.server.storage;

import java.util.Properties;

import poke.server.storage.jdbc.DatabaseStorage;

public class ServerManagementUtil {

	
	private static Properties dbProperties;
	private static String serverPort;
	private static Storage dbStorage;

	public static void setDBConnection(Properties props, String serverPort) {
		dbProperties = props;
		System.out.println("The port number inside Server management util : ============>" + serverPort);
		//ServerManagementUtil.serverPort = serverPort;
		ServerManagementUtil.serverPort = serverPort;
	}
	
	
	public static Storage getDatabaseStorage() {
		if (dbStorage == null) {
			System.out.println("db storage is null");
			dbStorage = new DatabaseStorage(dbProperties);
		} else {
			System.out.println("db storage is NOOOOOOOOOOOOOOOOOOOOTTTTTTTTTTTTTTT null");
		}
		return dbStorage;
	}
	
	public static String getServerPort() {
		return serverPort;
	}
}
