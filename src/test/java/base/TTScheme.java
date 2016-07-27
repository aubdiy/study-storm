package base;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-06-29 16:56
 */
public class TTScheme implements Scheme {
    public static final String STRING_SCHEME_KEY = "str";

    public TTScheme() {
    }

    public List<Object> deserialize(byte[] bytes) {
        return new Values(new Object[]{deserializeString(bytes)});
    }

    public static String deserializeString(byte[] string) {
        try {
            System.out.println(">>>>>>>>>>>>"+new String(string, "UTF-8"));
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException var2) {
            throw new RuntimeException(var2);
        }
    }

    public Fields getOutputFields() {
        return new Fields(new String[]{"str"});
    }
}
