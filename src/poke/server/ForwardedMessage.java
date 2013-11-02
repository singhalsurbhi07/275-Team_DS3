package poke.server;

public class ForwardedMessage {
  private String toNode;
  private com.google.protobuf.GeneratedMessage msg;
  public ForwardedMessage(String toNode,
      com.google.protobuf.GeneratedMessage msg) {
    this.toNode = toNode;
    this.msg = msg;
  }
  public String getToNode() {
    return toNode;
  }
  public void setToNode(String toNode) {
    this.toNode = toNode;
  }
  public com.google.protobuf.GeneratedMessage getMsg() {
    return msg;
  }
  public void setMsg(com.google.protobuf.GeneratedMessage msg) {
    this.msg = msg;
  }

}
