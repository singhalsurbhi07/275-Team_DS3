/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.storage.jdbc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.storage.Storage;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import eye.Comm.Document;
import eye.Comm.NameSpace;
import eye.Comm.Request;

public class DatabaseStorage implements Storage {
    protected static Logger logger = LoggerFactory.getLogger("database");

    public static final String sDriver = "jdbc.driver";
    public static final String sUrl = "jdbc.url";
    public static final String sUser = "jdbc.user";
    public static final String sPass = "jdbc.password";
    protected Properties cfg;
    protected BoneCP cpool;

    protected DatabaseStorage() {
    }

    public DatabaseStorage(Properties cfg) {
	init(cfg);
    }

    @Override
    public void init(Properties cfg) {
	if (cpool != null)
	    return;

	System.out.println("cfg properties =============>" + cfg);
	this.cfg = cfg;
	System.out.println(this.cfg);

	try {
	    System.out.println("property" + cfg.getProperty(sDriver));
	    Class.forName(cfg.getProperty(sDriver));
	    BoneCPConfig config = new BoneCPConfig();
	    config.setJdbcUrl(cfg.getProperty(sUrl));
	    config.setUsername(cfg.getProperty(sUser));
	    config.setPassword(cfg.getProperty(sPass));
	    config.setMinConnectionsPerPartition(5);
	    config.setMaxConnectionsPerPartition(10);
	    config.setPartitionCount(1);

	    cpool = new BoneCP(config);
	} catch (Exception e) {
	    System.out.println("Exception in creating the connection");
	    e.printStackTrace();
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see gash.jdbc.repo.Repository#release()
     */
    @Override
    public void release() {
	if (cpool == null)
	    return;

	cpool.shutdown();
	cpool = null;
    }

    @Override
    public NameSpace getNameSpaceInfo(long spaceId) {
	NameSpace space = null;

	Connection conn = null;
	try {
	    conn = cpool.getConnection();
	    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
	    // TODO complete code to retrieve through JDBC/SQL
	    // select * from space where id = spaceId
	} catch (Exception ex) {
	    ex.printStackTrace();
	    logger.error("failed/exception on looking up space " + spaceId, ex);
	    try {
		conn.rollback();
	    } catch (SQLException e) {
	    }
	} finally {
	    if (conn != null) {
		try {
		    conn.close();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	return space;
    }

    @Override
    public List<NameSpace> findNameSpaces(NameSpace criteria) {
	List<NameSpace> list = null;

	Connection conn = null;
	try {
	    conn = cpool.getConnection();
	    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
	    // TODO complete code to search through JDBC/SQL
	} catch (Exception ex) {
	    ex.printStackTrace();
	    logger.error("failed/exception on find", ex);
	    try {
		conn.rollback();
	    } catch (SQLException e) {
	    }
	} finally {
	    if (conn != null) {
		try {
		    conn.close();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	return list;
    }

    @Override
    public NameSpace createNameSpace(NameSpace space) {
	if (space == null)
	    return space;

	Connection conn = null;
	try {
	    conn = cpool.getConnection();
	    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
	    // TODO complete code to use JDBC
	} catch (Exception ex) {
	    ex.printStackTrace();
	    logger.error("failed/exception on creating space " + space, ex);
	    try {
		conn.rollback();
	    } catch (SQLException e) {
	    }

	    // indicate failure
	    return null;
	} finally {
	    if (conn != null) {
		try {
		    conn.close();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	}

	return space;
    }

    @Override
    public boolean removeNameSpace(long spaceId) {

	// TODO Auto-generated method stub
	return false;
    }

    private static java.sql.Date getCurrentDate() {
	java.util.Date today = new java.util.Date();
	return new java.sql.Date(today.getTime());
    }

    @Override
    public boolean addDocument(Request request, String filePath, String serverPort) {
	System.out
		.println("The port number is **************** inside database storage---------"
			+ serverPort);

	Connection conn = null;
	String docName = request.getBody().getDoc().getDocName();
	long fileSize = request.getBody().getDoc().getDocSize();

	String origin = request.getHeader().getOriginator();
	/*
	 * System.out.println("Origin is " + origin); String [] splitArray =
	 * origin.split(":");
	 */
	String host = null;
	try {
	    host = InetAddress.getLocalHost().getHostAddress();
	} catch (UnknownHostException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	}

	boolean result = true;
	try {
	    conn = cpool.getConnection();
	    String checkQuery = "select * from file_info where file_name = ?";
	    PreparedStatement chkStmt = conn.prepareStatement(checkQuery);
	    chkStmt.setString(1, docName);
	    ResultSet rs = chkStmt.executeQuery();
	    if (rs.next()) {
		updateDocument(request, filePath);
	    } else {
		// String query =
		// "Insert into file_info(file_name,file_size,server_ip,server_port,file_path) values(?,?,?,?,?)";
		String query = "Insert into file_info(file_name,file_size,server_ip,server_port,file_path) values(?,?,?,?,?)";
		PreparedStatement stmnt = conn.prepareStatement(query);
		stmnt.setString(1, docName);
		// stmnt.setString(2, namespace);
		stmnt.setLong(2, fileSize);
		stmnt.setString(3, host);
		stmnt.setString(4, serverPort);
		stmnt.setString(5, filePath);
		stmnt.execute();
	    }
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    result = false;
	    e.printStackTrace();
	} finally {
	    try {
		conn.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}

	// TODO Auto-generated method stub
	return result;
    }

    @Override
    public boolean removeDocument(String namespace, long docId) {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean updateDocument(String namespace, Document doc) {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean updateDocument(Request request, String filePath) {
	Connection conn = null;
	String docName = request.getBody().getDoc().getDocName();
	long fileSize = request.getBody().getDoc().getDocSize();
	boolean resultUpdate = true;
	try {
	    conn = cpool.getConnection();
	    // String query =
	    // "Insert into file_info(file_name,file_size,server_ip,server_port,file_path) values(?,?,?,?,?)";

	    String query = "update file_info set file_size = ?, created_date = ? where file_name = ?";
	    PreparedStatement stmnt = conn.prepareStatement(query);
	    stmnt.setLong(1, fileSize);
	    stmnt.setDate(2, getCurrentDate());
	    stmnt.setString(3, docName);
	    stmnt.executeUpdate();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    resultUpdate = false;
	    e.printStackTrace();
	} finally {
	    try {
		conn.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	return resultUpdate;
    }

    @Override
    public List<Document> findDocuments(String namespace, Document criteria) {
	// TODO Auto-generated method stub
	return null;
    }
}
