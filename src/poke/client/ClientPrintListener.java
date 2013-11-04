package poke.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.util.ClientUtil;
import poke.demo.Route;
import eye.Comm.Header;

/**
 * example listener that an application would use to receive events.
 * 
 * @author gash
 * 
 */
public class ClientPrintListener implements ClientListener {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String id;
	public String DOWNLOAD_DIR = null;
	
	public ClientPrintListener(String id) {
		this.id = id;
		Properties p = System.getProperties();
		String key = "user.home";
		String  USER_DIR = (String) p.get(key);
		 DOWNLOAD_DIR= USER_DIR + "/downloads";
		 
		 
		File clientFolder = new File(DOWNLOAD_DIR);
			if (!clientFolder.exists()) {
				if (clientFolder.mkdir()) {
					System.out.println("Directory is created!");
				} else {
					System.out.println("Directory already exist");
				}
			}
	}

	@Override
	public String getListenerID() {
		return id;
	}

	@Override
	public void onMessage(eye.Comm.Response msg) {
		System.out.println("In clientPrintListener");
		if (logger.isDebugEnabled()){
			ClientUtil.printHeader(msg.getHeader());
			System.out.println("INSIDE IS DEBUG ENABLED");
		}

		if (msg.getHeader().getRoutingId() == Header.Routing.FINGER){
			ClientUtil.printFinger(msg.getBody().getFinger());
			System.out.println("INSIDE FINGER");
		}

		else if (msg.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCFIND) {
				
				System.out.println("<<<<<<<<<<<<<<inside DOC find to write into client file system>>>>>>>>>>>>>>>>>>>>>>>");
				
				String fileName = msg.getBody().getStats()
						.getDocName();
				System.out.println("1 in client printlistender");
				String fileContent = msg.getBody().getStats()
						.getChunkContent().toStringUtf8();
				System.out.println("2 in client printlistender");
		
				System.out.println("Download directory path is ----------------->" + DOWNLOAD_DIR);
				File f = new File(DOWNLOAD_DIR + File.separator
						+ fileName);
				try {
					/*FileOutputStream fo = new FileOutputStream(f);
					byte[] b = msg.getBody().getStats()
							.getChunkContent().toByteArray();
					
					fo.write(b);
					fo.close();*/
					PrintWriter writer;
					writer = new PrintWriter(f, "UTF-8");
					writer.println(fileContent);
					writer.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out
						.println("The file content is ------------------------->"
								+ fileContent);
			} else if (msg.getHeader().getRoutingId() == eye.Comm.Header.Routing.STATS) {
				System.out.println(msg.getBody().getStats().getDocName()
						+ " uploaded");
			}else{
				System.out.println("No resorce ID set");
			// for (int i = 0, I = msg.getBody().getDocsCount(); i < I; i++)
			// ClientUtil.printDocument(msg.getBody().getDocs(i));
		}
	}
}
