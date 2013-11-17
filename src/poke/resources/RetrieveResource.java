package poke.resources;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ForwardQ;
import poke.server.ForwardedMessage;
import poke.server.Server;
import poke.server.conf.ServerConf;
import poke.server.resources.Resource;
import poke.server.storage.ServerManagementUtil;
import poke.server.vo.FileInfo;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
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

    public ServerConf getCfg() {
	return cfg;
    }

    private byte[] fileContent;

    public RetrieveResource() {

    }

    public void setCfg() {
	this.cfg = Server.getConf();
    }

    private Request createRequest(Request request) {
	RoutingPath rp = RoutingPath.newBuilder()
		.setNode(Server.getConf().getServer().getProperty("node.id"))
		.setTime(System.currentTimeMillis()).build();

	System.out.println("Retrieve Resource Adding Route Path");
	System.out.println(rp);
	// int hopCount = (int) request.getHeader().getRemainingHopCount();
	// hopCount--;

	/*
	 * Header newHeader = Header.newBuilder(request.getHeader())
	 * .setTime(System.currentTimeMillis())
	 * .setRemainingHopCount(hopCount).addPath(rp).build();
	 */
	Header newHeader = Header.newBuilder(request.getHeader())
		.setTime(System.currentTimeMillis())
		.addPath(rp).build();

	Request newRequest = Request.newBuilder(request).setHeader(newHeader)
		.build();

	for (RoutingPath rp1 : newHeader.getPathList()) {
	    System.out.println("added pathlist");
	    System.out.println(rp1);
	}

	return newRequest;
    }

    private Response createResponseFailure(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());

	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.FAILURE)
		.setReplyMsg(
			"Not able to retreive file")
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.setToNode(request.getHeader().getOriginator()).build();

	PayloadReply pb = PayloadReply.newBuilder().build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    private Response createResponseSuccess(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.SUCCESS)
		.setReplyMsg("Found the file")
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.setToNode(request.getHeader().getOriginator()).build();

	PayloadReply pb = PayloadReply.newBuilder().build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();

    }

    private String determineForwardNode(Request request) {
	System.out.println("Retreive resource: determine next node");
	List<RoutingPath> rp = request.getHeader().getPathList();
	String current = Server.getConf().getServer().getProperty("node.id");
	int i = 0;
	for (RoutingPath eachrp : rp) {
	    if (eachrp.getNode().equals(current)) {
		break;
	    }
	    i++;
	}
	// int index = rp.indexOf(current);
	// System.out.println("Retrieve resource: determine next node current index="
	// + index);
	System.out.println("Retrieve resource: determine next node current index=" + i);
	RoutingPath next = rp.get(i + 1);
	if (next != null) {
	    String nextNode = next.getNode();
	    System.out.println("Retrieve resource: determine next node next node=" + nextNode);

	    return nextNode;
	}
	return null;

	// System.out
	// .println("IN RETRIEVE RESOURCE ===============> determineForwardNode start");
	// List<RoutingPath> paths = request.getHeader().getPathList();
	// Collection<NodeDesc> neighboursList = cfg.getNearest()
	// .getNearestNodes().values();
	//
	// // System.out.println("IN determineForwardNode1");
	// if (paths == null || paths.size() == 0) {
	// System.out.println("paths==null, picking first nearest node");
	// System.out.println("NearestNode:"
	// + cfg.getNearest().getNearestNodes().values());
	// // pick first nearest
	// NodeDesc nd = cfg.getNearest().getNearestNodes().values()
	// .iterator().next();
	// if (nd == null)
	// System.out.println("nodedesc is null");
	//
	// System.out.println("RETRIEVE Resource: if path==null"
	// + nd.getNodeId());
	// return nd.getNodeId();
	// } else {
	// System.out
	// .println("RETRIEVE Resource:determine nextnode if path!=null");
	// // if this server has already seen this message return null
	//
	// for (NodeDesc nd : neighboursList) {
	// boolean found = true;
	// for (RoutingPath rp : paths) {
	// if (rp.getNode().equalsIgnoreCase(nd.getNodeId())) {
	// found = false;
	// break;
	// }
	// }
	// if (found) {
	// return nd.getNodeId();
	// }
	// }
	// }
	// return null;
    }

    @Override
    public Response process(Request request) {
	if (request.getHeader().getToNode()
		.equals(Server.getConf().getServer().getProperty("node.id"))) {
	    // Properties p = System.getProperties();
	    // Document retrievedFile = null;
	    String fileToBeRetrieved = request.getBody().getDoc().getDocName();
	    System.out
		    .println("The name of the file to be retrieved is ==============> "
			    + fileToBeRetrieved);

	    FileInfo fileInfo = ServerManagementUtil.getDatabaseStorage()
		    .findDocument(request, fileToBeRetrieved);

	    if (fileInfo != null) {
		String filePath = fileInfo.getFilePath();
		String fileName = fileInfo.getFileName();
		System.out
			.println("&&&&&&&&&&&&&& THE FILE PATH IS &&&&&&&&&&&&&77"
				+ filePath);
		try {
		    /*
		     * retrievedFile = Document.newBuilder()
		     * .setChunkContent(readFileAsByteString
		     * (filePath)).setDocName(fileName).build(); retrievePayload
		     * = Payload.newBuilder();
		     * retrievePayload.setDoc(retrievedFile);
		     */

		    Response res = createResponseSuccess(request);
		    Document d = Document.newBuilder().setDocName(fileName)
			    .setChunkContent(readFileAsByteString(filePath))
			    .build();
		    eye.Comm.PayloadReply.Builder pd = PayloadReply.newBuilder();
		    pd.setStats(d);

		    Response.Builder resp = res.toBuilder().setBody(pd.build());
		    return resp.build();

		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    } else {
		return createResponseFailure(request);
	    }
	} else {

	    setCfg();
	    if (cfg != null) {
		// adjacentNode =
		// cfg.getNearest().getNearestNodes().values().iterator().next().getNodeId();
		adjacentNode = determineForwardNode(request);
	    } else {
		System.out
			.println("------------------CFG IS NULL-------------------------");
	    }
	    System.out
		    .println("nextNode in retrieve resource=================>"
			    + adjacentNode);
	    GeneratedMessage msg = null;
	    ForwardedMessage fwdMessage;
	    if (adjacentNode != null) {
		msg = createRequest(request);
		// return null;
	    } else {
		// msg = createResponseFailure(request);
		return createResponseFailure(request);

		// return msg..

	    }
	    fwdMessage = new ForwardedMessage(adjacentNode, msg);
	    ForwardQ.enqueueRequest(fwdMessage);

	}

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
