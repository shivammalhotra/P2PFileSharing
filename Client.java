import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Client {
    static int totalChunks = 12;
    // private static byte[][] chunkStore;
    private static boolean[] chunkList;
    private static HashMap<Integer, byte[]> chunkRepo;
    static int ownPortNum;
    static int neighborPortNum;
    private static int clientId;
    private static int byteCount;
    static boolean flag = false; 
    private static int chunkCount;

    public static void main(String[] argv) throws Exception {
        TotalChunksHandler chunkHandle = new TotalChunksHandler();
         new Thread(chunkHandle).start();
         System.out.println("Started chunkhandler Thread");
         Thread.sleep(5000);

        clientId = Integer.parseInt(argv[0]);
        
        String fileName = "config.txt";
        String line = null;
         try {
             FileReader fileReader = new FileReader(fileName);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             while((line = bufferedReader.readLine()) != null) {
                 String[] array = line.split(",");
                 int id = Integer.parseInt(array[0]);
                 if(clientId == id){
                     ownPortNum = Integer.parseInt(array[1]);
                     neighborPortNum = Integer.parseInt(array[2]);
                 }
             }   
             System.out.println( "ClientId " + clientId + " ownPortNum " + ownPortNum + " neighborPortNum " + neighborPortNum);
             bufferedReader.close();         
         }
         catch(FileNotFoundException ex) {
             System.out.println("Unable to open file '" +fileName + "'");                
         }
         catch(IOException ex) {
             System.out.println("Error reading file '" + fileName + "'");                  
         }

        getChunksFromServer();
        System.out.println("Got chunks from Server");
        UpLoadHandler upHandle = new UpLoadHandler();
        new Thread(upHandle).start();
        System.out.println("Started Uploader Thread");
        DownLoadHandler downHandle = new DownLoadHandler();
        new Thread(downHandle).start();
        System.out.println("Started Downloader Thread");
    }

    public static void getChunksFromServer() throws Exception {
        int current = 0;
        //int clientId = 0;
        Socket sock = new Socket("127.0.0.1", 6798);
        DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
        dout.writeInt(clientId);
        InputStream is = sock.getInputStream();
        byte[] mybytearray = new byte[100 * 1024];
        int bytesRead = is.read(mybytearray, 0, mybytearray.length);
        current = bytesRead;
        while (bytesRead > -1) {
            if (current == 102400) {
                System.out.println(bytesRead +","+ current);
                int id = (int) mybytearray[0];
                // System.out.println("new chunk " + id);
                byteCount = byteCount + 102400;
                getChunkRepo().put(id, mybytearray);
                getChunkList()[id] = true;
                mybytearray = new byte[100 * 1024];
                current = 0;
                
            } else {
                System.out.println(bytesRead +","+ current);
                bytesRead = is.read(mybytearray, current,
                        (mybytearray.length - current));
                if (bytesRead >= 0)
                    current += bytesRead;
            }
        }
        if (current != 0) {
            byteCount = byteCount + current;
            byte[] mybytearraylast = new byte[current];
            System.arraycopy(mybytearray, 0, mybytearraylast, 0, current);
            System.out.println("Current : " + current + " ByteCount : "
                    + byteCount);
            int id = (int) mybytearraylast[0];
          
            getChunkRepo().put(id, mybytearraylast);
            getChunkList()[id] = true;
              System.out.println("new chunk " + id + " Chunk Length " + getChunkRepo().get(id).length);
        }
        System.out.println(current);
        chunkStatus();
        sock.close();

    }

    public static boolean[] getChunkList() {
        if (chunkList == null) {
            chunkList = new boolean[totalChunks];
            return chunkList;
        } else {
            return chunkList;
        }
    }

    public static HashMap<Integer, byte[]> getChunkRepo() {
        if (chunkRepo == null) {
            chunkRepo = new HashMap<Integer, byte[]>();
            return chunkRepo;
        } else {
            return chunkRepo;
        }
    }

    public static int getCount() throws Exception {
        int count = 0;
        for (int i = 0; i < chunkList.length; i++) {
            if (chunkList[i] == true) {
                count++;
            }
        }
        return count;
    }
    
    public static void chunkStatus() throws Exception {
        System.out.print("list of chunks currently on client ");
        int count = 0;
      
        for (int i = 0; i < chunkList.length; i++) {
            if (chunkList[i] == true) {
                System.out.print(i + "  ");
                count++;
            }
        }
        System.out.println();
       /* if(count==totalChunks && flag == false){
            flag = true;
            writeFile();
            System.out.println("writing to file");
        }*/
    }

    public static void writeFile() throws Exception {
        int start = 0;
        int end = 0;
        System.out.println("ByteCount " + byteCount);
        int size = 0;
        for (int i = 0; i < totalChunks; i++) {
            System.out.println("Chunk id "+ i + " size " + chunkRepo.get(i).length);
            size = size + chunkRepo.get(i).length;
        }
        System.out.println("ByteCount " + size);
        byte[] fullChunkArray = new byte[size];
        for (int i = 0; i < totalChunks; i++) {
            end = end +  chunkRepo.get(i).length -1;
            System.out.println("start : " + start + " End : " + end
                    + " chunkLength " + i + " : " + chunkRepo.get(i).length);
            System.arraycopy(chunkRepo.get(i), 1, fullChunkArray, start,
                    chunkRepo.get(i).length-1);
            start = end;
            flag = true;
        }

        FileOutputStream fos = new FileOutputStream(clientId + "_final.zip");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        bos.write(fullChunkArray, 0, fullChunkArray.length);
        bos.close();
    }
}

class DownLoadHandler implements Runnable {

    @Override
    public void run() {
        int current = 0;
        int byteCount = 0;
        int clientId = 0;
        while (true) {
            try {
                Socket sock = new Socket("127.0.0.1", Client.neighborPortNum);
                Timer timer = new Timer(); 
                timer.schedule(new RequestTask(sock,timer), 0, 5000); //delay in milliseconds
                while (true) {
                    try{
                        InputStream is = sock.getInputStream();
                        byte[] mybytearray = new byte[100 * 1024];
                        int bytesRead = is.read(mybytearray, 0, mybytearray.length);
                        current = bytesRead;
                        while (bytesRead > -1) {
                            if (current == 102400) {
                                System.out.println(bytesRead +","+ current);
                                int id = (int) mybytearray[0];
                                System.out.println("Received a new chunk id " + id);
                                byteCount = byteCount + 102400;
                                Client.getChunkRepo().put(id, mybytearray);
                                Client.getChunkList()[id] = true;
                                mybytearray = new byte[100 * 1024];
                                current = 0;
                            } else {
                                bytesRead = is.read(mybytearray, current,
                                        (mybytearray.length - current));
                                if (bytesRead >= 0)
                                    current += bytesRead;
                            }
                        }
                        if (current != 0) {
                            byteCount = byteCount + current;
                            byte[] mybytearraylast = new byte[current];
                            System.arraycopy(mybytearray, 0, mybytearraylast, 0,
                                    current);
                            System.out.println("Current : " + current + " ByteCount : "
                                    + byteCount);
                            int id = (int) mybytearraylast[0];
                            System.out.println("new chunk " + id);
                            Client.getChunkRepo().put(id, mybytearraylast);
                            Client.getChunkList()[id] = true;
                    }
                    }catch(Exception e){
                        System.out.println(e);
                    }
                    
                    System.out.println("No chunks for download yet. Retry in a while");
                    if(Client.getCount() == Client.totalChunks){
                        try {
                          System.out.println("DownLoading done");  
                          if(Client.flag==false){
                            Client.writeFile();
                          }
                           timer.cancel();
                            timer.purge();
                          sock.close();
                          return;
                          
                        }catch(IOException ioe) {
                          System.out.println("Error closing client connection");
                        }
                    }

                   
                }

            } catch (Exception ne) {
                System.out.println("Neighbouring peer not active yet");
               /* try{
                    Thread.sleep(1000);
                }catch(Exception k){
                    System.out.println("Cannot sleep");
                }*/
                
            }

        }
        
    }

    public static void request(Socket sock) throws Exception{
        DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
        for (int i = 0; i < Client.totalChunks; i++) {
            Thread.sleep(1000);
            if (Client.getChunkList()[i] == false) {
                dout.writeInt(i);
                System.out.println("Request Sent for chunk " + i
                        + "to  the neighbour");
            }

        }

    }

}

class UpLoadHandler implements Runnable {

    @Override
    public void run(){
        try{
            ServerSocket servsock = new ServerSocket(Client.ownPortNum);
            System.out.println("Listening for download requests");
            while (true) {
                Socket sock = servsock.accept();
                try {
                    DataInputStream dis = new DataInputStream(sock.getInputStream());
                    OutputStream os = sock.getOutputStream();
                    while (true) {
                        int chunkId = dis.readInt();
                        if (Client.getChunkRepo().containsKey(chunkId)) {
                            byte[] chunk = Client.getChunkRepo().get(chunkId);
                            if(chunk.length == 102400){
                                os.write(chunk, 0, chunk.length);
                                os.flush();
                                System.out.println("Sent chunk " + chunkId
                                    + " to neighbour "+" size " + chunk.length);
                            }else{
                                System.out.println("Chunk id "+ chunkId+" Get Count " + Client.getCount() +" total Chunks "+Client.totalChunks);
                                if(Client.getCount() == Client.totalChunks){
                                    System.out.println("Going to send the last one");
                                    os.write(chunk, 0, chunk.length);
                                    os.flush();
                                    System.out.println("Sent chunk " + chunkId
                                        + " to neighbour "+" size " + chunk.length);
                                    try {
                                        sock.close();
                                        break;
                                    }catch(IOException ioe) {
                                        System.out.println("Error closing client connection");
                                  }
                                }
                            }   
                        }
                    }
                    /*try {
                          sock.close();
                      }catch(IOException ioe) {
                          System.out.println("Error closing client connection");
                    }*/
                    
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }catch(IOException ioe){
            System.out.println(ioe);
        }
        
    }
}

class RequestTask extends TimerTask {
    Socket sock;
    Timer timer;
    
    public RequestTask(Socket sock, Timer timer){
        this.sock = sock;
        this.timer = timer;
    }
    @Override
    public void run() {
        try{
            DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
            for (int i = 0; i < Client.totalChunks; i++) {
                if (Client.getChunkList()[i] == false) {
                    dout.writeInt(i);
                    //System.out.println("Request Sent for chunk " + i
                            //+ "to  the neighbour");
                }
            }
            if(Client.getCount() == Client.totalChunks && Client.flag == false){
                Client.writeFile();
                timer.cancel();
                timer.purge();
                return;
            }
            Client.chunkStatus();
        }catch(IOException ioe){
            System.out.println(ioe);
        }catch(Exception e){
            System.out.println(e);
        }
       
    }
}


class TotalChunksHandler implements Runnable{

    @Override
    public void run() {
       
       	Socket clientsock = null;
      	try{
            clientsock = new Socket("127.0.0.1", 9052);
            DataInputStream dis = new DataInputStream(clientsock.getInputStream());
            System.out.println("test");
            Client.totalChunks = dis.readByte() + 1;
            System.out.println("Received numchunks " + Client.totalChunks);        
      	}catch(Exception e){
            System.out.println(e);
        }finally{
        	try{
        		clientsock.close();
        	}catch(IOException ioe){
        		System.out.println(ioe);
        	}
        	
        }
                
    }
    
}