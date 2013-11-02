package poke.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.ClientListener;

public class PeerListener implements ClientListener {

  protected static Logger logger = LoggerFactory.getLogger("PeerListener");

  private String id;

  public PeerListener(String id) {
    this.id = id;
  }

  public void onMessage(eye.Comm.Response res) {
    System.out.println("In peer listener");
    System.out.println(res.getHeader().getReplyMsg());

    /*
     * if (res.getHeader().getToNode()
     * .equalsIgnoreCase(Server.getConf().getServer().getProperty("node.id"))) {
     * ChannelQueue queue = QueueFactory.getInstance(ch);
     * queue.enqueueResponse(res); } else { ForwardedMessage msg = new
     * ForwardedMessage(res.getHeader().getToNode(), res);
     * ForwardQ.enqueueResponse(msg); }
     */
  }
  @Override
  public String getListenerID() {
    // TODO Auto-generated method stub
    return this.id;
  }
}
