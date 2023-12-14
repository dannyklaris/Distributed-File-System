public class DstoreInfo {
    private String dstoreName;
    private int dstorePort;
    private int dstoreNumber;
    private String fileFolder;
    public DstoreInfo(String dstoreName, int dstorePort, String fileFolder) {
        this.dstoreName = dstoreName;
        this.dstorePort = dstorePort;
        this.fileFolder = fileFolder;
    }

    public DstoreInfo(int dstorePort, String fileFolder) {
        this.dstorePort = dstorePort;
        this.fileFolder = fileFolder;
    }

    public String getDstoreName() {
        return dstoreName;
    }

    public int getDstorePort() {
        return dstorePort;
    }

    public String getFileFolder() {
        return fileFolder;
    }

    public int getDstoreNumber() {
        return dstoreNumber;
    }
}
