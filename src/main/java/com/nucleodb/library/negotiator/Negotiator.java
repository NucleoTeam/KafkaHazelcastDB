package com.nucleodb.library.negotiator;

public class Negotiator {
  // request from node for hashes.
  void requestHashes(){
    // send to topic a request for hashes
  }
  // confirm if node can take hash from this node
  boolean confirmHashSwap(int size){ // current hash size
    // confirm that this node can handle a hash of this size
    return false;
  }

  // send new hash to topic and let a node take it
  void newHash(){

  }

  // send response to a hash request.
  void sendHash(){
    // calculate what hashes
  }

}
