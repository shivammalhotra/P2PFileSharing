import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Server {
   private static int fileLength = 0;
   static int numChunks = 0;
   
  public static void main(String[] args) throws IOException {
    ServerSocket servsock = new ServerSocket(6798);
    File myFile = new File("store/small.zip");
    fileLength = (int) myFile.length();
    byte[] mybytearray = new byte[(int) myFile.length()];
    ArrayList<byte[]> files = splitFiles(myFile);
    TotalChunksHandler chunkHandle = new TotalChunksHandler();
    new Thread(chunkHandle).start();
    System.out.println("Started chunkhandler Thread");
    while (true) {
        Socket sock = servsock.accept();
        ClientHandler cHandle = new ClientHandler(sock, files, numChunks); 
        new Thread(cHandle).start();
    }
  }

  public static ArrayList<byte[]> splitFiles(File myFile) throws IOException{
    ArrayList<byte[]> splitFiles = new ArrayList<byte[]>();
    int chunkLength = 102400;
    numChunks = (int) Math.ceil(fileLength/chunkLength);
    int start = 0;
    int end = chunkLength;
    System.out.println("Filelength : " + fileLength + " chunkNum : " + numChunks);
    byte[] mybytearray = new byte[(int) myFile.length()];
    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(myFile));
    bis.read(mybytearray, 0, mybytearray.length);
    for(int i = 0; i<numChunks;i++){
      System.out.println("Stage :" + i  + " Start :" + start+" End : "+end);
      byte[] chunkArray = new byte[chunkLength];
      chunkArray[0] = (byte)i;
      System.out.println("stage :" + i + " first byte val : " + (int)chunkArray[0]);
      System.arraycopy(mybytearray , start, chunkArray, 1, chunkLength-1);
      splitFiles.add(i,chunkArray);
      start = end;
      end += chunkLength-1;
    }
    byte[] chunkArray = new byte[fileLength-start+2];
    chunkArray[0] = (byte)(numChunks);
    end = fileLength-start+1;
    System.out.println("Stage :" + numChunks  + " Start :" + start+" End : "+ end);
    System.arraycopy(mybytearray , start, chunkArray, 1, fileLength-start);
    int chunkfileLength = (int)chunkArray.length;
    System.out.println(numChunks*102400+chunkfileLength);
    splitFiles.add(numChunks,chunkArray);
    return splitFiles;  
  }
}

class ClientHandler implements Runnable{

  private Socket sock;
  private ArrayList<byte[]> files;
  private int numChunks;
  private int numClients = 5;
  
  public ClientHandler(Socket sock, ArrayList<byte[]> files, int numChunks){
    this.sock = sock;
    this.files = files;
    this.numChunks = numChunks;
  }
  
  @Override
  public void run() {
    try{
      DataInputStream dis = new DataInputStream(sock.getInputStream());
        OutputStream os = sock.getOutputStream();
        int clientId = dis.readInt();
        int counter = clientId;
        System.out.println("ClientId " + clientId + " Counter " + counter + " numchunks " + numChunks);
        while(counter<=numChunks){
          byte[] chunk = files.get(counter);
          os.write(chunk, 0, chunk.length);
          os.flush();
          System.out.println("Sent chunk "+counter+" to clinet " + clientId + " size " + chunk.length);
          counter += numClients;
        }
      /* for(int i = 0;i<=numChunks;i++){
          byte[] chunk = files.get(i);
          os.write(chunk, 0, chunk.length);
          os.flush();
          System.out.println("Sent chunk "+counter+" to clinet ");
        }*/
        try {
              sock.close();
          }catch(IOException ioe) {
              System.out.println("Error closing client connection");
        }
    }catch(IOException ioException){
      ioException.printStackTrace();
    }
  }
}


class TotalChunksHandler implements Runnable{

    @Override
    public void run() {
          try{
                ServerSocket servsock = new ServerSocket(9052);
                while(true){
                     Socket sock = servsock.accept();
                     DataOutputStream dis = new DataOutputStream(sock.getOutputStream());
                     dis.writeByte(Server.numChunks);
                     dis.flush();
                     System.out.println("Sent numchunks " + Server.numChunks);
                     
                }
          }catch(Exception e){
                System.out.println(e);
            }
                
    }
    
}