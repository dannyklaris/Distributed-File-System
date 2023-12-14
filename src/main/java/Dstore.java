import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class Dstore {
    private int port;
    private int cport;
    private int timeout;
    private String fileFolder;
    private String dstoreName;
    private ConcurrentHashMap<String, ControllerServiceThread> cstThread;
    private ConcurrentHashMap<Integer, ControllerServiceThread> cstThread1;

    private InputStream dstoreIn;
    private OutputStream dstoreOut;

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.cstThread = new ConcurrentHashMap<>();
        this.cstThread1 = new ConcurrentHashMap<>();
    }

    public void start() {
        try {
            Socket controllerSocket = new Socket("localhost", cport);
            Thread controllerThread = new Thread(new ControllerServiceThread(controllerSocket));
            controllerThread.start();
            // Listen for requests from the Client
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Dstore listening on dstore port: " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                Thread clientThread = new Thread(new ClientServiceThread(clientSocket));
                clientThread.start();
            }

        } catch (IOException e) {
            System.err.println("Error accepting Client connection: " + e);
            e.printStackTrace();
        }
    }

    class ControllerServiceThread implements Runnable {
        Socket controllerSocket;
        BufferedReader controllerIn;
        PrintWriter controllerOut;


        ControllerServiceThread(Socket c) {
            controllerSocket = c;
        }

        public BufferedReader getControllerIn() {
            return controllerIn;
        }

        public PrintWriter getControllerOut() {
            return controllerOut;
        }

        @Override
        public void run() {
            try {
                controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);

                // Send the connection information to the Controller
                controllerOut.println(Protocol.JOIN_TOKEN + " " + port + " " + fileFolder);
                cstThread1.put(port,this);
                System.out.println(Protocol.JOIN_TOKEN + " " + port + " " + fileFolder + " is sent to Controller.");

                String cmessage;
                while ((cmessage = controllerIn.readLine()) != null) {
                    String[] cmessageParts = cmessage.split(" ");
                    System.out.println();
                    System.out.println(Arrays.toString(cmessageParts) + " received from Controller.");

                    if (cmessageParts[0].equals(Protocol.REMOVE_TOKEN)) {
                        removeRequest(fileFolder, cmessageParts[1],controllerOut);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error on controller connection: " + e);
                e.printStackTrace();
            }
        }
    }

    class ClientServiceThread implements Runnable {
        Socket clientSocket;
        BufferedReader clientIn;
        PrintWriter clientOut;
        ClientServiceThread(Socket c) {
            clientSocket = c;
        }

        @Override
        public void run() {
            try {
                clientIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                clientOut = new PrintWriter(clientSocket.getOutputStream(), true);
                ControllerServiceThread cst1 = cstThread1.get(port);
                System.out.println();
                System.out.println("Client is connected to dstore port: " + port);
                String message;
                while ((message = clientIn.readLine()) != null) {
                    String[] messageParts = message.split(" ");
                    System.out.println(Arrays.toString(messageParts) + " received from client.");
                    if (messageParts[0].equals(Protocol.STORE_TOKEN)) {

                        if (cst1 != null) {
                            PrintWriter controllerOut = cst1.getControllerOut();
                            storeRequest(fileFolder, messageParts[1],clientOut,controllerOut, clientSocket);
                        }

                    } else if (messageParts[0].equals(Protocol.LOAD_DATA_TOKEN)) {
                        if (cst1 != null) {
                            loadRequest(fileFolder, messageParts[1], clientSocket);
                        }

                    }
                }
            } catch (IOException e) {
                System.err.println("Error in client connection: " + e);
                e.printStackTrace();
            }
        }
    }

    private void createFileFolder(String fileFolder) throws IOException {
        File folder = new File(fileFolder);

        if (!folder.exists()) {
            if (folder.mkdirs()) {
                System.out.println("File folder created successfully: " + folder.getAbsolutePath());
            } else {
                throw new IOException("Failed to create file folder: " + folder.getAbsolutePath());
            }
        } else {
            System.out.println("File folder already exists: " + folder.getAbsolutePath());
        }
    }

    private void storeRequest(String fileFolder, String filename,PrintWriter clientOut, PrintWriter controllerOut, Socket socket) throws IOException {
        clientOut.println(Protocol.ACK_TOKEN);
        System.out.println("ACK is sent to client.");
        // Create the file folder if it doesn't exist


        File outputFile = new File(fileFolder, filename);

        try (OutputStream dstoreOut = new FileOutputStream(outputFile)) {

            dstoreIn = socket.getInputStream();

            byte[] buffer = new byte[1024];

            int bytesRead;

            while ((bytesRead = dstoreIn.readNBytes(buffer, 0, buffer.length)) > 0) {
                dstoreOut.write(buffer, 0, bytesRead);
            }

            System.out.println(filename + " is stored in " + fileFolder + ".");
            // Once finished storing the file, send STORE_ACK to the controller
            controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
            System.out.println("STORE_ACK is sent to controller.");
        } catch (IOException e) {
            System.err.println("Error storing file or closing streams: " + e);
        }
    }

    private void loadRequest(String fileFolder, String filename, Socket socket) throws IOException {
        File inputFile = new File(fileFolder, filename);
        // Read the file content and send it to the client
        try (FileInputStream fileInputStream = new FileInputStream(inputFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            dstoreOut = socket.getOutputStream();
            while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                dstoreOut.write(buffer, 0, bytesRead);
            }
            System.out.println(filename + " is loaded from " + fileFolder + ".");
        } catch (IOException e) {
            System.err.println("Error loading file: " + e);
        }
    }

    private void removeRequest(String fileFolder, String filename, PrintWriter controllerOut) {
        Path filePath = Paths.get(fileFolder, filename);

        try {
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                Files.delete(filePath);
                System.out.println("File " + filename + " deleted successfully.");
            } else {
                controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                System.out.println("File " + filename + " does not exist.");
            }
        } catch (IOException e) {
            System.err.println("Failed to delete file " + filename + ": " + e.getMessage());
        }
        controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        System.out.println("REMOVE_ACK " + filename + " is sent to controller.");
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        Dstore dstore = new Dstore(port,cport,timeout,fileFolder);
        dstore.createFileFolder(fileFolder);
        dstore.start();

    }
}
