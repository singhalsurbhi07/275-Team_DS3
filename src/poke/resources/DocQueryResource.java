package poke.resources;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.ForwardQ;
import poke.server.ForwardedMessage;
import poke.server.Server;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.resources.Resource;
import poke.server.storage.ServerManagementUtil;
import poke.server.vo.FileInfo;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.RoutingPath;

public class DocQueryResource implements Resource {

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

    public DocQueryResource() {

    }

    public void setCfg() {
	this.cfg = Server.getConf();
    }

    private Request createRequest(Request request) {
	RoutingPath rp = RoutingPath.newBuilder()
		.setNode(Server.getConf().getServer().getProperty("node.id"))
		.setTime(System.currentTimeMillis()).build();

	System.out.println("DocQueryResource:createRequest for forwarding  Adding Route Path");
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
			"Not enough hop counts or not able to determine next node")
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.setToNode(request.getHeader().getOriginator()).build();

	Document d = Document.newBuilder().setDocName(request.getBody().getDoc().getDocName())
		.build();

	PayloadReply pb = PayloadReply.newBuilder().setStats(d).build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();
    }

    private Response createResponseSuccess(Request request) {

	// fb.setTag(request.getBody().getFinger().getTag());
	RoutingPath rp = RoutingPath.newBuilder()
		.setNode(Server.getConf().getServer().getProperty("node.id"))
		.setTime(System.currentTimeMillis()).build();
	Header fb = Header
		.newBuilder(request.getHeader())
		.setReplyCode(ReplyStatus.SUCCESS)
		.setReplyMsg("Found the file")
		.setOriginator(
			Server.getConf().getServer().getProperty("node.id"))
		.setToNode(request.getHeader().getOriginator()).addPath(rp).build();

	Document d = Document.newBuilder().setDocName(request.getBody().getDoc().getDocName())
		.build();

	PayloadReply pb = PayloadReply.newBuilder().setStats(d).build();
	return Response.newBuilder().setBody(pb).setHeader(fb).build();

    }

    private String determineForwardNode(Request request) {
	System.out
		.println("IN RETRIEVE RESOURCE ===============> determineForwardNode start");
	List<RoutingPath> paths = request.getHeader().getPathList();
	Collection<NodeDesc> neighboursList = cfg.getNearest()
		.getNearestNodes().values();

	// System.out.println("IN determineForwardNode1");
	if (paths == null || paths.size() == 0) {
	    System.out.println("paths==null, picking first nearest node");
	    System.out.println("NearestNode:"
		    + cfg.getNearest().getNearestNodes().values());
	    // pick first nearest
	    NodeDesc nd = cfg.getNearest().getNearestNodes().values()
		    .iterator().next();
	    if (nd == null)
		System.out.println("nodedesc is null");

	    System.out.println("RETRIEVE Resource: if path==null"
		    + nd.getNodeId());
	    return nd.getNodeId();
	} else {
	    System.out
		    .println("RETRIEVE Resource:determine nextnode if path!=null");
	    // if this server has already seen this message return null

	    for (NodeDesc nd : neighboursList) {
		boolean found = true;
		for (RoutingPath rp : paths) {
		    if (rp.getNode().equalsIgnoreCase(nd.getNodeId())) {
			found = false;
			break;
		    }
		}
		if (found) {
		    return nd.getNodeId();
		}
	    }
	}
	return null;
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
	if (fileInfo != null) {
	    System.out.println("DocQueryResource:fileinfo not null");

	    Response res = createResponseSuccess(request);
	    System.out.println("DocQueryResource:forward the response" + res);

	    return res;
	} else {

	    /*
	     * for (NodeDesc nn : cfg.getNearest().getNearestNodes().values()) {
	     * adjacentNode = nn.getNodeId(); }
	     */
	    setCfg();
	    if (cfg != null) {
		if (request.getHeader().getIsExternal()) {
		    adjacentNode = determineExternalNode(request);
		    System.out.println("DocQueryResource:adjacent node when is external=true :"
			    + adjacentNode);

		} else {

		    adjacentNode = determineForwardNode(request);
		    System.out.println("DocQueryResource:adjacent node when is external=false :"
			    + adjacentNode);

		}
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

	logger.info("++++++++++++++++++++++ after building response ++++++++++++++++++++++");
	System.out
		.println("THE--------------------RESPONSE------------------IS"
			+ response);
	/*
	 * else { // file is not present in this node. // you will have to
	 * prepare an appropriate response // take the next node and set it to
	 * the response. }
	 */
	return null;
    }

    private String determineExternalNode(Request request) {
	NodeDesc nd = cfg.getExternal().getExternalNodes().values()
		.iterator().next();
	if (nd == null) {
	    System.out.println("nodedesc is null");
	    return null;
	}

	System.out.println("RETRIEVE Resource: if path==null"
		+ nd.getNodeId());
	return nd.getNodeId();

    }

}
