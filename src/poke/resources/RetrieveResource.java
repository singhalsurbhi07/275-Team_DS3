package poke.resources;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ForwardQ;
import poke.server.ForwardQWorker;
import poke.server.ForwardedMessage;
import poke.server.Server;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatData;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import poke.server.storage.ServerManagementUtil;
import poke.server.vo.FileInfo;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.RoutingPath;

public class RetrieveResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("server");
	public static final String sDriver = "jdbc.driver";
	public static final String sUrl = "jdbc.url";
	public static final String sUser = "jdbc.user";
	public static final String sPass = "jdbc.password";
	private ServerConf cfg;
	public String adjacentNode = null;
	
	public ServerConf getCfg(){
		return cfg;
	}

	private byte[] fileContent;

	public RetrieveResource() {

	}
	
	public void setCfg(){
		this.cfg = Server.getConf();
	}

	private Request createRequest(Request request) {
	    RoutingPath rp = RoutingPath.newBuilder()
	        .setNode(Server.getConf().getServer().getProperty("node.id"))
	        .setTime(System.currentTimeMillis()).build();

	    System.out.println("Retrieve Resource Adding Route Path");
	    System.out.println(rp);
	    int hopCount = (int) request.getHeader().getRemainingHopCount();
	    hopCount--;
	    
	    Header newHeader = Header
	        .newBuilder(request.getHeader())
	        .setTime(System.currentTimeMillis())
	        .setRemainingHopCount(hopCount).addPath(rp)
	        .build();

	    Request newRequest = Request.newBuilder(request).setHeader(newHeader)
	        .build();

	    for (RoutingPath rp1 : newHeader.getPathList()) {
	        System.out.println("added pathlist");
	        System.out.println(rp1);
	    }

	    return newRequest;
	    }
	
	private Response createResponse(Request request) {

	    // fb.setTag(request.getBody().getFinger().getTag());
	    Header fb = Header
	        .newBuilder(request.getHeader())
	        .setReplyCode(ReplyStatus.FAILURE)
	        .setReplyMsg(
	            "Not enough hop counts or not able to determine next node")
	        .setOriginator(Server.getConf().getServer().getProperty("node.id"))
	        .setToNode(request.getHeader().getOriginator())
	        .build();

	    PayloadReply pb = PayloadReply.newBuilder()
	        .build();
	    return Response.newBuilder().setBody(pb).setHeader(fb).build();
	    }
	
	@Override
	public Response process(Request request) {
		Properties p = System.getProperties();
		Document retrievedFile = null;
		String fileToBeRetrieved = request.getBody().getDoc().getDocName();
		System.out
				.println("The name of the file to be retrieved is ==============> "
						+ fileToBeRetrieved);

		FileInfo fileInfo = ServerManagementUtil.getDatabaseStorage()
				.findDocument(request, fileToBeRetrieved);
		Response response = null;
		eye.Comm.PayloadReply.Builder retrievePayload = null;
		if(fileInfo != null){
			String filePath = fileInfo.getFilePath();
			String fileName = fileInfo.getFileName();
			System.out.println("&&&&&&&&&&&&&& THE FILE PATH IS &&&&&&&&&&&&&77" + filePath);
			try {
				/*retrievedFile = Document.newBuilder()
						.setChunkContent(readFileAsByteString(filePath)).setDocName(fileName).build();
				retrievePayload = Payload.newBuilder();
				retrievePayload.setDoc(retrievedFile);*/
				
				Response res=createResponse(request);
				Document d = Document.newBuilder().setDocName(fileName)
				        .setChunkContent(readFileAsByteString(filePath)).build();
				eye.Comm.PayloadReply.Builder pd = PayloadReply.newBuilder();
				pd.setStats(d);
				
				Response.Builder resp=res.toBuilder().setBody(pd.build());
				return resp.build();
				
	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			
			/*for (NodeDesc nn : cfg.getNearest().getNearestNodes().values()) {
			      adjacentNode = nn.getNodeId();
			    }*/
				setCfg();
				if(cfg != null){
				adjacentNode = cfg.getNearest().getNearestNodes().values().iterator().next().getNodeId();
				}
				else{
					System.out.println ("------------------CFG IS NULL-------------------------");
				}
		      System.out.println("nextNode in retrieve resource=================>" + adjacentNode );
		      GeneratedMessage msg = null;

		      if (adjacentNode != null ) {
		          msg = createRequest(request);
		      } 
		      else{
		    	 msg = createResponse(request);
		      }
		      
		      ForwardedMessage fwdMessage = new ForwardedMessage(adjacentNode, msg);
		      ForwardQ.enqueueRequest(fwdMessage);
			
			
		}
			/*retrievedFile = Document.newBuilder()
					.setChunkContent(readFileAsByteString(filePath)).setDocName(fileName).build();
			retrievePayload = Payload.newBuilder();
			retrievePayload.setDoc(retrievedFile);*/
		
			//Response.Builder rb = Response.newBuilder();

            // metadata
			/*Header header = ResourceUtil.buildHeaderFrom(request.getHeader(),
                    ReplyStatus.SUCCESS, "File Retrieved", Server.getConf().getServer().getProperty("node.id"));
			*/
			//Header header = ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.SUCCESS, "File Retrieved");
			//Header.Builder h = header.toBuilder();
			//h.setOriginator(Server.getConf().getServer().getProperty("node.id"));
			//rb.setHeader(h);

            
            //PayloadReply.Builder payloadReply = PayloadReply.newBuilder();
            
           // if(retrievedFile != null){
            	
            //payloadReply.addDocs(retrievedFile);
            //}
            
            //rb.setBody(payloadReply.build());
            
            
            //rb.setBody(payloadReply.build());
            
            //Response reply = rb.build();
            //return rb;

/*
			
			Response.Builder res = Response.newBuilder();
			res.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(),
					ReplyStatus.SUCCESS, null));
			PayloadReply.Builder payBuild = PayloadReply.newBuilder();
			Document.Builder docBuild = Document.newBuilder();
			docBuild.setDocName(request.getBody().getDoc().getDocName());
			docBuild.s
			payBuild.setStats(docBuild.build());
			payBuild
            response = Response.newBuilder().setHeader(h.build())
					.setBody(payloadReply.build()).build();
 */			

            //response = rb.build();
			logger.info("++++++++++++++++++++++ after building response ++++++++++++++++++++++");
			System.out.println("THE--------------------RESPONSE------------------IS" +response);
		/*else {
			// file is not present in this node.
			// you will have to prepare an appropriate response
			// take the next node and set it to the response.
		}*/
		return null;
}		
	


	public ByteString readFileAsByteString(String fileLocation)
			throws IOException {

		System.out
				.println("///////////////////This is inside readFileAsByteString//////////////////////");
		FileInputStream fileToRead = new FileInputStream(fileLocation);
		byte[] fileContent = new byte[fileToRead.available()];
		fileToRead.read(fileContent);
		fileToRead.close();
		return ByteString.copyFrom(fileContent);

	}

}
