package spout;

/**
 * @author liujinxin
 * @since 2015-06-19 15:57
 */
public class KafkaBroker {
    private String host;
    private int port;


    public KafkaBroker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
