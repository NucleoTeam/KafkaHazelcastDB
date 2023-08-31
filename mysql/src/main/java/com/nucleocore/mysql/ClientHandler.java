package com.nucleocore.mysql;

import com.nucleocore.library.database.utils.Serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serial;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.nucleocore.mysql.MySQLConstants.CLIENT_CONNECT_ATTRS;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_CONNECT_WITH_DB;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_DEPRECATE_EOF;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_FOUND_ROWS;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_LONG_FLAG;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_LONG_PASSWORD;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_PLUGIN_AUTH;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_PROTOCOL_41;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_SECURE_CONNECTION;
import static com.nucleocore.mysql.MySQLConstants.CLIENT_SESSION_TRACK;
import static com.nucleocore.mysql.MySQLConstants.SERVER_SESSION_STATE_CHANGED;

public class ClientHandler implements Runnable{
  private final SocketChannel socketChannel;
  private String username;
  private static final String PASSWORD = "test";


  public ClientHandler(SocketChannel socketChannel) {
    this.socketChannel = socketChannel;
  }

  @Override
  public void run() {
    try {
      sendHandshakeV10();
      handlePackets();
      //sendAuthSwitchRequest();
//      if (handleAuthSwitchResponse()) {
//        System.out.println("WOOOT");
//      }
      //sendOkPacket();
    } catch (Exception e) {
      e.printStackTrace();
    }
    /*try (
        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
    ) {
      String clientInput;
      while ((clientInput = reader.readLine()) != null) {
        System.out.println("Received: " + clientInput);

        if (clientInput.trim().toUpperCase().startsWith("SELECT")) {
          out.println("id\tname");
          out.println("1\tAlice");
          out.println("2\tBob");
          out.println("3\tCharlie");
          out.println("Query OK, 3 rows in set");
        } else {
          out.println("Query OK, 0 rows affected");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }*/
  }

  void handlePackets() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    while (true) {
      buffer.clear();
      int bytesRead = socketChannel.read(buffer);
      if (bytesRead == -1) {
        break;
      }
      buffer.flip();

      // Read packet length (3 bytes)
      int packetLength = buffer.get() & 0xFF;
      packetLength |= (buffer.get() & 0xFF) << 8;
      packetLength |= (buffer.get() & 0xFF) << 16;
      Serializer.log(packetLength);

      // Read sequence ID (1 byte)
      byte sequenceId = buffer.get();

      // Read packet type (1 byte)
      byte packetType = buffer.get();


      // Handle packet based on the packet type
      switch (packetType) {
        case 0x00: // Protocol::OK_Packet
          handleOkPacket(buffer, packetLength, sequenceId);
          break;
        case (byte) 0xFF: // Protocol::ERR_Packet
          handleErrPacket(buffer, packetLength, sequenceId);
          break;
        case (byte)0xA4: // Protocol::HandshakeResponse41
          handleHandshakeResponse41(buffer, packetLength, sequenceId);
          break;
        case (byte) 0xFE: // Protocol::EOF_Packet
          //handleEOFPacket(buffer, packetLength, sequenceId);
          break;
        case 0x02: // Protocol::ResultSetHeaderPacket
          //handleResultSetHeaderPacket(buffer, packetLength, sequenceId);
          break;
        case 0x03: // Protocol::FieldPacket
          //handleFieldPacket(buffer, packetLength, sequenceId);
          break;
        case 0x04: // Protocol::RowDataPacket
          //handleRowDataPacket(buffer, packetLength, sequenceId);
          break;
        case 0x05: // Protocol::PrepareStatementOKPacket
          //handlePrepareStatementOKPacket(buffer, packetLength, sequenceId);
          break;
        case 0x08: // Protocol::StatementExecutePacket
          //handleStatementExecutePacket(buffer, packetLength, sequenceId);
          break;
        case 0x0F: // Protocol::ColumnDefinition41Packet
          //handleColumnDefinition41Packet(buffer, packetLength, sequenceId);
          break;
        // Add more cases for other packet types if needed
        default:
          System.out.println("Unsupported packet type: 0x" + Integer.toHexString(packetType & 0xFF));
          break;
      }
    }
  }

  String byteArrayToHexString(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      hexString.append(String.format("%02x", b));
    }
    return hexString.toString();
  }
  private byte[] salt;

  String generateSalt(){
    String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    // Initialize a StringBuilder to hold the random string
    StringBuilder sb = new StringBuilder();

    // Create a new Random object
    Random random = new Random();

    // Generate a random string of length 20
    for (int i = 0; i < 20; i++) {
      int randomIndex = random.nextInt(alphabet.length());
      sb.append(alphabet.charAt(randomIndex));
    }

    // Convert the StringBuilder to a String and print it
    return sb.toString();
  }
  int connection = 0;
  byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) +
          Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }
  byte[] concatenateBytes(byte[] a, byte[] b) {
    byte[] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
  public static boolean validateMySQLNativePassword(byte[] clientHash, byte[] salt, String storedPassword) throws NoSuchAlgorithmException {
    // Step 1: Hash the password
    byte[] passwordHashStep1 = sha1(storedPassword.getBytes());

    // Step 2: Hash the hash from step 1
    byte[] passwordHashStep2 = sha1(passwordHashStep1);

    // Step 3: Hash the salt
    byte[] saltHash = sha1(salt);

    // Step 4: XOR the hash from step 2 with the hash of the salt
    byte[] toBeXORD = passwordHashStep2;
    for (int i = 0; i < toBeXORD.length; i++) {
      toBeXORD[i] ^= saltHash[i];
    }

    // Step 5: Hash the result of the XOR operation
    byte[] passwordHashStep3 = sha1(toBeXORD);

    Serializer.log(passwordHashStep3);

    // Step 6: Compare the received hash from the client with the expected hash
    return Arrays.equals(clientHash, passwordHashStep3);
  }

  public static byte[] sha1(byte[] input) throws NoSuchAlgorithmException {
    MessageDigest mDigest = MessageDigest.getInstance("SHA1");
    return mDigest.digest(input);
  }
  String byteToHex(byte[] byteArray){
    StringBuilder hexString = new StringBuilder(2 * byteArray.length);
    for (byte b : byteArray) {
      String hex = Integer.toHexString(0xFF & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return byteArray.toString();
  }

  void sendHandshakeV10() throws IOException {

    salt = generateSalt().getBytes(StandardCharsets.UTF_8);

    String first = byteArrayToHexString(Arrays.copyOfRange(salt, 0, 8));
    String last = byteArrayToHexString(Arrays.copyOfRange(salt, 8, 20));

    Serializer.log(first);
    Serializer.log(last);

    byte[] data = hexStringToByteArray(("71 00 00 00 0a 35 2e 35 2e 35 2d 31 30 2e 31 30" +
        "  2e 32 2d 4d 61 72 69 61    44 42 2d 31 3a 31 30 2e" +
        "  31 30 2e 32 2b 6d 61 72    69 61 7e 75 62 75 32 32" +
        "  30 34 00 25 00 00 00 "+first+" 00" +
        "  fe f7 2d 02 00 ff 81 15    00 00 00 00 00 00 1d 00" +
        "  00 00 "+last+" 00 6d" +
        "  79 73 71 6c 5f 6e 61 74    69 76 65 5f 70 61 73 73" +
        "  77 6f 72 64 00").replace(" ", ""));

    socketChannel.write(ByteBuffer.allocate(data.length).put(data).flip());
  }

  void handleHandshakeResponse41(ByteBuffer buffer, int packetLength, byte sequenceId) {
    // Read the HandshakeResponse41 packet

    // Read the client capabilities (4 bytes)
    int clientCapabilities = buffer.getInt();
    System.out.println("Client Capabilities: " + clientCapabilities);

    // Read the max packet size (4 bytes)
    int maxPacketSize = buffer.getInt();
    System.out.println("Max Packet Size: " + maxPacketSize);

    // Read the character set (1 byte)
    byte characterSet = buffer.get();
    System.out.println("Character Set: " + characterSet);

    // Read the reserved bytes (23 bytes)
    byte[] reservedBytes = new byte[22];
    buffer.get(reservedBytes);
    System.out.println("Reserved Bytes: " + Arrays.toString(reservedBytes));

    // Read the username (null-terminated string)
    StringBuilder username = new StringBuilder();
    byte b;
    while ((b = buffer.get()) != 0) {
      username.append((char) b);
    }
    System.out.println("Username: " + username.toString());

    int len = buffer.get();
    byte[] authData = new byte[len];
    buffer.get(authData);

    try {
      Serializer.log(validateMySQLNativePassword(authData, salt, PASSWORD));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    Serializer.log(new String(authData, StandardCharsets.UTF_8));

    // Perform the required actions based on the handshake response
    // Implement your logic here
  }
  void handleOkPacket(ByteBuffer buffer, int packetLength, byte sequenceId) {
    // Read the OK_Packet

    // Read the affected rows (variable-length integer)
    long affectedRows = readVariableLengthInteger(buffer);
    System.out.println("Affected Rows: " + affectedRows);

    // Read the last insert ID (variable-length integer)
    long lastInsertId = readVariableLengthInteger(buffer);
    System.out.println("Last Insert ID: " + lastInsertId);

    // Read the status flags (2 bytes)
    int statusFlags = buffer.getShort() & 0xFFFF;
    System.out.println("Status Flags: " + statusFlags);

    // Read the warnings (2 bytes)
    int warnings = buffer.getShort() & 0xFFFF;
    System.out.println("Warnings: " + warnings);

    // Perform the required actions based on the OK_Packet
    // Implement your logic here
  }

  long readVariableLengthInteger(ByteBuffer buffer) {
    int length = buffer.get() & 0xFF;
    if (length < 0xFB) {
      return length;
    } else if (length == 0xFB) {
      return -1; // NULL value
    } else if (length == 0xFC) {
      return buffer.getShort() & 0xFFFF;
    } else if (length == 0xFD) {
      return buffer.getInt() & 0xFFFFFFFFL;
    } else if (length == 0xFE) {
      return buffer.getLong();
    } else {
      throw new IllegalArgumentException("Invalid variable-length integer");
    }
  }
  void handleErrPacket(ByteBuffer buffer, int packetLength, byte sequenceId) {
    // Read the ERR_Packet

    // Read the error code (2 bytes)
    int errorCode = buffer.getShort() & 0xFFFF;
    System.out.println("Error Code: " + errorCode);

    // Read the SQL state marker (1 byte)
    byte sqlStateMarker = buffer.get();
    System.out.println("SQL State Marker: " + sqlStateMarker);

    // Read the SQL state (5 bytes)
    byte[] sqlStateBytes = new byte[5];
    buffer.get(sqlStateBytes);
    String sqlState = new String(sqlStateBytes, StandardCharsets.UTF_8);
    System.out.println("SQL State: " + sqlState);

    // Read the error message (null-terminated string)
    StringBuilder errorMessage = new StringBuilder();
    byte b;
    while ((b = buffer.get()) != 0) {
      errorMessage.append((char) b);
    }
    System.out.println("Error Message: " + errorMessage.toString());

    // Perform the required actions based on the ERR_Packet
    // Implement your logic here
  }
  void createOkPacket(byte sequenceId, long affectedRows, long lastInsertId, int statusFlags, int warnings) throws IOException {
    // Prepare the OK_Packet
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Packet length will be recalculated later
    buffer.put(new byte[]{0, 0, 0});

    // Sequence ID
    sequenceId += 1;
    buffer.put(sequenceId);

    // OK_Packet header (0x00)
    buffer.put((byte) 0x00);

    // Affected rows (variable-length integer)
    writeVariableLengthInteger(buffer, affectedRows);

    // Last insert ID (variable-length integer)
    writeVariableLengthInteger(buffer, lastInsertId);

    // Status flags (2 bytes)
    buffer.putShort((short) statusFlags);

    // Warnings (2 bytes)
    buffer.putShort((short) warnings);

    // Calculate and update packet length
    int packetLength = buffer.position() - 4;
    buffer.put(0, (byte) (packetLength & 0xFF));
    buffer.put(1, (byte) ((packetLength >> 8) & 0xFF));
    buffer.put(2, (byte) ((packetLength >> 16) & 0xFF));

    // Flip the buffer for writing
    buffer.flip();

    // Send the OK_Packet to the client
    socketChannel.write(buffer);
  }

  void writeVariableLengthInteger(ByteBuffer buffer, long value) {
    if (value < 0xFB) {
      buffer.put((byte) value);
    } else if (value <= 0xFFFF) {
      buffer.put((byte) 0xFC);
      buffer.putShort((short) value);
    } else if (value <= 0xFFFFFF) {
      buffer.put((byte) 0xFD);
      buffer.putInt((int) value);
    } else {
      buffer.put((byte) 0xFE);
      buffer.putLong(value);
    }
  }
}