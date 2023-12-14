import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class Controller {

    private int cport;
    private int replication_factor;
    private int timeout;
    private int rebalance_period;
    private int counter;
    String dstoreName;
    String fileFolder;
    private Map<Integer, DstoreInfo> dstoreindex1;
    private ConcurrentHashMap<String, DstoreServiceThread> dstoreThreads;
    private ConcurrentHashMap<Integer,DstoreServiceThread> dstoreThreads1;
    private Map<String, FileInfo> fileindex;
    List<String> dstoreNames;
    List<Integer> dstorePorts;

    public Controller(int cport, int replication_factor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.replication_factor = replication_factor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        this.dstoreindex1 = new HashMap<>();
        this.dstoreThreads = new ConcurrentHashMap<>();
        this.dstoreThreads1 = new ConcurrentHashMap<>();
        this.fileindex = new HashMap<>();
        this.dstoreNames = Collections.synchronizedList(new ArrayList<>());
        this.dstorePorts = Collections.synchronizedList(new ArrayList<>());

    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(cport);
            System.out.println("Controller listening on controller port: " + cport);
            System.out.println();
            for (;;) {
                try {
                    Socket client = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line = in.readLine();
                    if (line != null) {
                        String[] lineParts = line.split(" ");
                        System.out.println(Arrays.toString(lineParts)+ " received from general socket.");
                        if (lineParts[0].equals("JOIN")) {
                            new Thread(new DstoreServiceThread(client,line,dstoreName)).start();
                        } else {
                            new Thread(new ClientServiceThread(client,line,dstoreName)).start();
                        }
                    }

                } catch (IOException e) {
                    System.err.println(e);
                }
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    class DstoreServiceThread implements Runnable {
        Socket dstore;
        String firstLine;
        BufferedReader in;
        PrintWriter out;
        String dstoreName;
        int portNumber;
        private final Map<String,Boolean> storeAckReceived;
        private final Map<String,Boolean> removeAckReceived;

        DstoreServiceThread(Socket c, String firstLine, String dstoreName) {
            dstore = c;
            this.dstoreName = dstoreName;
            this.firstLine = firstLine;
            this.storeAckReceived = new HashMap<>();
            this.removeAckReceived = new HashMap<>();
        }

        DstoreServiceThread(Map<String, Boolean> storeAckReceived, Map<String, Boolean> removeAckReceived) {
            this.storeAckReceived = storeAckReceived;
            this.removeAckReceived = removeAckReceived;
        }

        public String getDstoreName() {
            return dstoreName;
        }
        public PrintWriter getOut() {
            return out;
        }
        public Map<String, Boolean> getStoreAckReceived() {
            return this.storeAckReceived;
        }
        public Map<String, Boolean> getRemoveAckReceived() {
            return this.removeAckReceived;
        }
        private void processStoreAck(String fileName) {
            synchronized (storeAckReceived) {
                storeAckReceived.put(fileName,true);
            }
        }
        private void processRemoveAck(String fileName) {
            synchronized (removeAckReceived) {
                removeAckReceived.put(fileName,true);
            }
        }

        @Override
        public void run() {
            try {
                in = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                out = new PrintWriter(dstore.getOutputStream(),true);
                String[] firstLinePart = firstLine.split(" ");
                if (firstLinePart[0].equals("JOIN")) {
                    portNumber = Integer.parseInt(firstLinePart[1]);
                    fileFolder = firstLinePart[2];
                    dstoreThreads1.put(portNumber,this);
                    dstorePorts.add(portNumber);
                    addDstore1(portNumber,fileFolder);

                }
                String line;
                while((line = in.readLine()) != null) {
                    String[] lineParts = line.split(" ");
                    System.out.println(Arrays.toString(lineParts) + " received from dstoreservicethread.");
                    if(lineParts[0].equals(Protocol.STORE_ACK_TOKEN)) {
                        String fileName = lineParts[1];
                        processStoreAck(fileName);
                    }
                    else if(lineParts[0].equals(Protocol.REMOVE_ACK_TOKEN)) {
                        String fileName = lineParts[1];
                        processRemoveAck(fileName);
                    }
                }
            } catch (IOException e) {
                System.err.println("Connection is dropped for dstore " + portNumber + ". dstore " + portNumber + " is removed from Dstore database." );
                System.err.println();
                synchronized (dstoreThreads1) {
                    dstoreThreads1.remove(this.portNumber);
                }
                synchronized (dstoreindex1) {
                    dstoreindex1.remove(this.portNumber);
                }
                synchronized (dstorePorts) {
                    dstorePorts.remove(0);
                }
            }
        }
    }

    class ClientServiceThread implements Runnable {
        Socket client;
        String firstLine;
        String dstoreName;
        int reloadCounter = 1;
        int counter;
        private Map<String, Boolean> storeAckReceived;
        private Map<String, Boolean> removeAckReceived;

        ClientServiceThread(Socket c, String firstLine, String dstoreName) {
            client = c;
            this.firstLine = firstLine;
            this.dstoreName = dstoreName;
            this.storeAckReceived = new HashMap<>();
            this.removeAckReceived = new HashMap<>();

        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(),true);
                String clientName = "client" + counter++;
                System.out.println(clientName + " is connected to Controller.");


                DstoreInfo dstoreInfo1 = getDstoreEntry1(dstorePorts.get(0));
                DstoreServiceThread dst1 = dstoreThreads1.get(dstoreInfo1.getDstorePort());

                for (DstoreInfo dstoreInfo2 : dstoreindex1.values()) {
                    System.out.println(dstoreInfo2.getDstorePort() + " is available for " + clientName + ".");
                }
                System.out.println();

                String[] firstLineParts = firstLine.split(" ");
                if(firstLineParts[0].equals(Protocol.LIST_TOKEN)) {
                    System.out.println(Arrays.toString(firstLineParts) + " is received from client.");
                    if (dstoreindex1.size() < replication_factor) {
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    listRequest(out,dstoreInfo1.getFileFolder());
                    System.out.println();
                }
                else if (firstLineParts[0].equals(Protocol.STORE_TOKEN)) {
                    String fileName = firstLineParts[1];
                    int filesize = Integer.parseInt(firstLineParts[2]);
                    if (dstoreindex1.size() < replication_factor) {
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    else {
                        long startTime = System.currentTimeMillis();
                        storeRequest(fileName, filesize, out);
                        System.out.println();
                        if (dst1 != null) {
                            boolean ackReceived = false;
                            while (System.currentTimeMillis() - startTime < timeout) {
                                if (dst1.getStoreAckReceived().getOrDefault(fileName,true)) {
                                    ackReceived = true;
                                    break;
                                }
                                Thread.sleep(100);
                            }
                            if (ackReceived) {
                                out.println(Protocol.STORE_COMPLETE_TOKEN);
                                FileInfo fileInfo = getFileEntry(fileName);
                                fileInfo.setStatus("STORE_COMPLETE");
                                System.out.println(fileInfo.getFilename() + " status in the file database has been set to " + fileInfo.getStatus());
                                System.out.println("STORE_COMPLETE is sent to client.");
                                System.out.println();
                            } else {
                                removeFile(fileName);
                                System.out.println("Timeout expired, " + fileName + " was removed from file database.");
                            }
                        }
                    }
                }
                else if (firstLineParts[0].equals(Protocol.LOAD_TOKEN) ) {
                    if (dstoreindex1.size() < replication_factor) {
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                    else {
                        loadRequest(firstLineParts[1],dstoreInfo1.getDstorePort(), out);
                        System.out.println();
                    }
                }

                else if (firstLineParts[0].equals(Protocol.RELOAD_TOKEN)) {
                    DstoreInfo dstoreInfo2 = getDstoreEntry1(dstorePorts.get(1));
                    if (dstoreindex1.size() < replication_factor) {
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    }
                     else {
                        loadRequest(firstLineParts[1],dstoreInfo1.getDstorePort(), out);
                        System.out.println();
                    }
                }

                else if (firstLineParts[0].equals(Protocol.REMOVE_TOKEN)) {
                    String fileName = firstLineParts[1];

                    if (dstoreindex1.size() < replication_factor) {
                        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    } else {
                        if (dst1 != null) {
                                removeRequest(fileName,dstoreThreads1,out);


                            while (!dst1.getRemoveAckReceived().getOrDefault(fileName,false)) {
                                FileInfo fileInfo = getFileEntry(fileName);
                                Thread.sleep(100);

                                if (fileInfo != null) {
                                    fileInfo.setStatus("REMOVE_COMPLETE");
                                    out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                    System.out.println(fileInfo.getFilename() + " status in the file database has been set to " + fileInfo.getStatus());
                                    System.out.println("REMOVE_COMPLETE is sent to client.");
                                    removeFile(fileName);
                                    System.out.println();
                                }


                            }
                        }

                    }
                }

                String line;
                while((line = in.readLine()) != null) {
                    String[] lineParts = line.split(" ");
                    System.out.println(Arrays.toString(lineParts) + " received from clientservicethread.");
                    if (lineParts[0].equals(Protocol.LIST_TOKEN)){
                        if (dstoreindex1.size() < replication_factor) {
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }
                        else {
                            listRequest(out, dstoreInfo1.getFileFolder());
                            System.out.println();
                        }

                    }
                    else if (lineParts[0].equals(Protocol.STORE_TOKEN)) {
                        String fileName = lineParts[1];
                        int filesize = Integer.parseInt(lineParts[2]);
                        if (dstoreindex1.size() < replication_factor) {
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }
                        else {
                            long startTime = System.currentTimeMillis();
                            storeRequest(fileName, filesize, out);
                            System.out.println();

                            if (dst1 != null) {
                            while (System.currentTimeMillis() - startTime < timeout && !dst1.getStoreAckReceived().getOrDefault(fileName,false)) {
                                Thread.sleep(100);
                                out.println("STORE_COMPLETE");
                                FileInfo fileInfo = getFileEntry(fileName);
                                fileInfo.setStatus("STORE_COMPLETE");
                                System.out.println(fileInfo.getFilename() + " status in the file database has been set to " + fileInfo.getStatus());
                                System.out.println("STORE_COMPLETE is sent to client.");
                                System.out.println();
                            }
                            }
                        }


                    }
                    else if (lineParts[0].equals(Protocol.LOAD_TOKEN)) {
                        if (dstoreindex1.size() < replication_factor) {
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            loadRequest(lineParts[1],dstoreInfo1.getDstorePort(), out);
                            System.out.println();
                        }
                    }
                    else if (lineParts[0].equals(Protocol.RELOAD_TOKEN)) {

                        if (reloadCounter < dstorePorts.size()) {
                            DstoreInfo dstoreInfo2 = getDstoreEntry1(dstorePorts.get(reloadCounter++));
                            loadRequest(lineParts[1],dstoreInfo2.getDstorePort(), out);
                            System.out.println();
                        }
                        else {
                            out.println(Protocol.ERROR_LOAD_TOKEN);
                            System.out.println("ERROR_LOAD is sent because client cannot connect to or receive data from any dstores.");
                        }

                    }
                    else if (lineParts[0].equals(Protocol.REMOVE_TOKEN)) {
                        String fileName = lineParts[1];

                        if (dstoreindex1.size() < replication_factor) {
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            if (dst1 != null) {


                                removeRequest(fileName,dstoreThreads1,out);


                                while (!dst1.getRemoveAckReceived().getOrDefault(fileName,false)) {
                                    FileInfo fileInfo = getFileEntry(fileName);
                                    Thread.sleep(100);

                                    if (fileInfo != null) {
                                        fileInfo.setStatus(Protocol.REMOVE_COMPLETE_TOKEN);
                                        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                        System.out.println(fileInfo.getFilename() + " status in the file database has been set to " + fileInfo.getStatus());
                                        System.out.println("REMOVE_COMPLETE is sent to client.");
                                        removeFile(fileName);
                                        System.out.println();
                                    }


                                }
                        }

                        }
                    }
                }

                dst1.getStoreAckReceived().clear();
                dst1.getRemoveAckReceived().clear();
            } catch (IOException e) {
                System.err.println(e);} catch (InterruptedException e) {
                System.err.println(e);
            }
        }
    }

    public void addDstore1(int dstorePort, String fileFolder) {
        DstoreInfo dstoreInfo1 = new DstoreInfo(dstorePort,fileFolder);
        dstoreindex1.put(dstorePort, dstoreInfo1);
        System.out.println("[" + dstorePorts + ", " + fileFolder + "]" + " is stored in the Dstore database.");
    }

    public DstoreInfo getDstoreEntry1 (int portNumber) {return dstoreindex1.get(portNumber);}

    public void addFile1(String filename, int size, String status, int portNumber) {
        FileInfo fileInfo = new FileInfo(filename,size,status, portNumber);
        fileindex.put(filename,fileInfo);
        System.out.println("[" + filename + ", " + size + ", " + status +  ", " + portNumber + "] " + "is stored in the File database.");
    }

    public void removeFile(String fileName) {
        fileindex.remove(fileName);
    }

    public FileInfo getFileEntry(String fileName) {
        return fileindex.get(fileName);
    }

    private void listRequest(PrintWriter out, String fileFolder) throws IOException {
        File folder = new File(fileFolder);
        File[] files = folder.listFiles();
        List<String> fileNames = new ArrayList<>();
        for (File file : files) {
            if (file.isFile()) {
                fileNames.add(file.getName());
            }
        }
        String fileList = String.join(" ", fileNames);
        out.println(Protocol.LIST_TOKEN + " " + fileList);
        System.out.println("[" + "LIST " + fileList + "]" + " is sent to client.");
    }

    private void storeRequest(String filename, int filesize, PrintWriter out) throws IOException {

        FileInfo fileInfo = getFileEntry(filename);
        if (fileInfo != null) {
            // File with the same filename already exists
            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            System.out.println("ERROR_FILE_ALREADY_EXISTS is sent to client.");
            return;
        }
        else if (fileInfo != null && fileInfo.getStatus().equals("STORE_IN_PROGRESS")) {
            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            System.out.println("ERROR_FILE_ALREADY_EXISTS is sent to client due to concurrent store operation.");
            return;
        }
        else {
            for (DstoreInfo dst1 : dstoreindex1.values()) {
                addFile1(filename, filesize, "STORE_IN_PROGRESS", dst1.getDstorePort());
            }
            StringBuilder ports = new StringBuilder();
            for (DstoreInfo dst1 : dstoreindex1.values()) {
                ports.append(dst1.getDstorePort()).append(" ");
            }
            out.println(Protocol.STORE_TO_TOKEN + " " + ports);
            System.out.println("STORE_TO " + ports + " is sent to client.");
        }

    }

    private void loadRequest(String filename, int dstorePort, PrintWriter out) throws IOException {

        FileInfo fileInfo = getFileEntry(filename);

        if (fileInfo == null) {
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            System.out.println("ERROR_FILE_DOES_NOT_EXIST is sent to client.");
            return;
        }
        else if (fileindex.containsKey(filename)) {

            int filesize = fileInfo.getSize();
            out.println(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + filesize);
            System.out.println("LOAD_FROM " + dstorePort + " " + filesize + " is sent to client.");
        }

    }

    private void removeRequest(String filename, ConcurrentHashMap<Integer, DstoreServiceThread> dstoreThreads, PrintWriter clientOut) throws IOException {
        FileInfo fileInfo = getFileEntry(filename);

        if (fileInfo == null) {
            clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            System.out.println("ERROR_FILE_DOES_NOT_EXIST is sent to client.");
            return;
        }
        else {
            fileInfo.setStatus("REMOVE_IN_PROGRESS");
            for (DstoreServiceThread dst : dstoreThreads1.values()) {
                dst.getOut().println(Protocol.REMOVE_TOKEN + " " + filename);
            }
            System.out.println("REMOVE " + filename + " is sent to Dstore.");
            // Remove the file from the fileindex
        }
    }


    public static void main(String[] args) {
        int cport = Integer.parseInt(args[0]);
        int replication_factor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);
        Controller controller1 = new Controller(cport,replication_factor,timeout,rebalance_period);
        controller1.start();
    }
}
