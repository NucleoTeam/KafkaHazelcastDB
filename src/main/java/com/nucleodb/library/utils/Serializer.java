package com.nucleodb.library.utils;

import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.*;
import java.nio.ByteBuffer;

public class Serializer {
  public static byte[] write(Object obj)  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      byte[] data = bos.toByteArray();
      return data;
    }catch (IOException e){
      e.printStackTrace();
    }finally {
      if(oos!=null) {
        try {
          oos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(bos!=null) {
        try {
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }
  public static <T> T read(byte[] data)  {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(bis);
      return (T) ois.readObject();
    }catch (IOException e){
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      if(ois!=null) {
        try {
          ois.close();
        } catch (IOException e) {
          //e.printStackTrace();
        }
      }
      if(bis!=null) {
        try {
          bis.close();
        } catch (IOException e) {
          //e.printStackTrace();
        }
      }
    }
    return null;
  }
}
