public class FileInfo {
    private String filename;
    private int size;
    private String status;
    private int port;
    private String dstoreName;

    public FileInfo(String filename, int size, String status, String dstoreName) {
        this.filename = filename;
        this.size = size;
        this.status = status;
        this.dstoreName = dstoreName;
    }
    public FileInfo(String filename, int size, String status, int port) {
        this.filename = filename;
        this.size = size;
        this.status = status;
        this.port = port;
    }

    public String getFilename(){
        return filename;
    }

    public int getSize() {
        return size;
    }

    public String getStatus() {
        return status;
    }

    public String getDstoreName() {
        return dstoreName;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
