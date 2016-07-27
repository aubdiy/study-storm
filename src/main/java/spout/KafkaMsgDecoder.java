package spout;

import backtype.storm.tuple.Fields;

import java.io.Serializable;
import java.util.List;

/**
 * kafka消息解码器
 *
 * @author liujinxin
 * @since 2015-06-22 18:42
 */
public interface KafkaMsgDecoder extends Serializable {

    /**
     * 生成storm中spout组件中的fields
     * @return
     */
    Fields generateFields();

    /**
     * 生成storm中spout组件需要emit的tuple
     * @param message
     * @return
     */
    List<Object> generateTuple(byte[] message);
}
