package self.aub.study.s05_trident.state;

import java.io.Serializable;

/**
 * @author liujinxin
 * @since 2015-07-19 22:08
 */
public class S05BasiTridentMetadata implements Serializable {
    private static final long serialVersionUID = 7538132169285218142L;
    long index;

    public long getIndex() {
        return index;
    }

    public S05BasiTridentMetadata(long index) {
        this.index = index;
    }

}