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
package poke.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang.RandomStringUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Finger;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.Request;

/**
 * provides an abstraction of the communication to the remote server.
 * 
 * @author gash
 * 
 */
public class ClientConnection {
	protected static Logger logger = LoggerFactory.getLogger("client");
	private static String originator = "zero";
	private String host;
	private int port;
	private ChannelFuture channel; // do not use directly call connect()!
	private ClientBootstrap bootstrap;
	ClientDecoderPipeline clientPipeline;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker worker;
	public static final int ID_LENGTH = 6;

	public static int count = 0;

	public String generateUniqueId() {
		return RandomStringUtils.randomAlphanumeric(ID_LENGTH);
	}

	protected ClientConnection(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	/**
	 * release all resources
	 */
	public void release() {
		bootstrap.releaseExternalResources();
	}

	public static ClientConnection initConnection(String host, int port) {

		ClientConnection rtn = new ClientConnection(host, port);
		return rtn;
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(ClientListener listener) {
		try {
			if (clientPipeline != null)
				clientPipeline.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	public void uploadFile(String namespace, String filePath, String dest)
			throws IOException {
		String[] names = filePath.split("/");
		int namesLength = names.length;
		String fileName = names[namesLength - 1];
		System.out.println("FileName =" + fileName);
		long fileSize = new File(filePath).length();
		System.out.println("The file size is = " + fileSize);
		if (namespace == null) {
			namespace = "server" + originator;
		}
		String id = generateUniqueId();

		Document d = Document.newBuilder().setDocName(fileName)
				.setChunkContent(getFileAsByteString(filePath))
				.setDocSize(fileSize).build();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setDoc(d);
		NameSpace ns = NameSpace.newBuilder().setName(namespace).build();
		p.setSpace(ns);

		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator(originator);
		h.setCorrelationId(id);
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.DOCADD);
		h.setRemainingHopCount(4);

		if (dest != null) {
			h.setToNode(dest);
		}
		Request r = Request.newBuilder().setHeader(h.build())
				.setBody(p.build()).build();

		eye.Comm.Request req = r;

		try {
			// enqueue message
			outbound.put(req);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

	public void retrieveFile(String fileName) {
		String id = generateUniqueId();

		System.out.println(" Inside retrieve file method in client connection");

		Document.Builder docBuilder = Document.newBuilder();
		docBuilder.setDocName(fileName);

		eye.Comm.Payload.Builder payloadBuilder = Payload.newBuilder();
		payloadBuilder.setDoc(docBuilder.build());

		Request.Builder requestBuilder = Request.newBuilder();
		requestBuilder.setBody(payloadBuilder.build());
		eye.Comm.Header.Builder requestHeader = Header.newBuilder();
		requestHeader.setOriginator(originator);
		requestHeader.setCorrelationId(id);
		requestHeader.setTime(System.currentTimeMillis());
		requestHeader.setRoutingId(eye.Comm.Header.Routing.DOCFIND);
		requestHeader.setRemainingHopCount(4);
		requestBuilder.setHeader(requestHeader.build());

		eye.Comm.Request fileRequest = requestBuilder.build();
		try {
			// enqueue message
			outbound.put(fileRequest);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

	public void findFile(String fileName) {
		String id = generateUniqueId();

		System.out.println("Inside find file method in Client connection");

		Document.Builder docBuilder = Document.newBuilder();
		docBuilder.setDocName(fileName);

		eye.Comm.Payload.Builder payloadBuilder = Payload.newBuilder();
		payloadBuilder.setDoc(docBuilder.build());

		Request.Builder requestBuilder = Request.newBuilder();
		requestBuilder.setBody(payloadBuilder.build());
		eye.Comm.Header.Builder requestHeader = Header.newBuilder();
		requestHeader.setOriginator(originator);
		requestHeader.setCorrelationId(id);
		requestHeader.setTime(System.currentTimeMillis());
		requestHeader.setRoutingId(eye.Comm.Header.Routing.DOCQUERY);
		requestBuilder.setHeader(requestHeader.build());

		eye.Comm.Request fileRequest = requestBuilder.build();
		try {
			// enqueue message
			outbound.put(fileRequest);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

	public void removeFile(String fileName) {
		String id = generateUniqueId();
		System.out.println("Inside rempve file method in Client connection");
		Document.Builder docBuilder = Document.newBuilder();
		docBuilder.setDocName(fileName);
		eye.Comm.Payload.Builder payloadBuilder = Payload.newBuilder();
		payloadBuilder.setDoc(docBuilder.build());
		Request.Builder requestBuilder = Request.newBuilder();
		requestBuilder.setBody(payloadBuilder.build());
		eye.Comm.Header.Builder requestHeader = Header.newBuilder();
		requestHeader.setOriginator(originator);
		requestHeader.setCorrelationId(id);
		requestHeader.setTime(System.currentTimeMillis());
		requestHeader.setRoutingId(eye.Comm.Header.Routing.DOCREMOVE);
		requestBuilder.setHeader(requestHeader.build());
		eye.Comm.Request fileRequest = requestBuilder.build();
		try {
			// enqueue message
			outbound.put(fileRequest);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

	public void poke(String tag, int num) throws IOException {
		// data to send
		Finger f = eye.Comm.Finger.newBuilder().setTag(tag).setNumber(num)
				.build();

		// payload containing data
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setFinger(f);

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.FINGER);

		Request r = Request.newBuilder().setHeader(h.build())
				.setBody(p.build()).build();

		eye.Comm.Request req = r;

		try {
			// enqueue message
			outbound.put(req);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}

	public ByteString getFileAsByteString(String filePath) throws IOException {
		FileInputStream input = new FileInputStream(filePath);

		byte[] fileData = new byte[input.available()];

		input.read(fileData);
		input.close();
		return ByteString.copyFrom(fileData);
	}

	private void init() {
		// the queue to support client-side surging
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		clientPipeline = new ClientDecoderPipeline();
		bootstrap.setPipelineFactory(clientPipeline);

		// start outbound message processor
		worker = new OutboundWorker(this);
		worker.start();
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			// System.out.println("---> connecting");
			channel = bootstrap.connect(new InetSocketAddress(host, port));
			// cleanup on lost connection
		}

		// wait for the connection to establish
		channel.awaitUninterruptibly();

		if (channel.isDone() && channel.isSuccess())
			return channel.getChannel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

	/**
	 * queues outgoing messages - this provides surge protection if the client
	 * creates large numbers of messages.
	 * 
	 * @author gash
	 * 
	 */
	protected class OutboundWorker extends Thread {
		ClientConnection conn;
		boolean forever = true;

		public OutboundWorker(ClientConnection conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException(
						"connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel ch = conn.connect();
			if (ch == null || !ch.isOpen()) {
				ClientConnection.logger
						.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && conn.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {
						ClientHandler handler = conn.connect().getPipeline()
								.get(ClientHandler.class);

						if (!handler.send(msg))
							conn.outbound.putFirst(msg);
					} else
						conn.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ClientConnection.logger.error(
							"Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ClientConnection.logger.info("connection queue closing");
			}
		}
	}
}
