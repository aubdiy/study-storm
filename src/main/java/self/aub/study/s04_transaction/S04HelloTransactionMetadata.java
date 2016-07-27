package self.aub.study.s04_transaction;

import java.io.Serializable;

/**
 * @author liujinxin
 * @since 2015-07-13 20:46
 */
public class S04HelloTransactionMetadata implements Serializable {
    private static final long serialVersionUID = 7538132169285218142L;
    int index;

    public int getIndex() {
        return index;
    }

    public S04HelloTransactionMetadata(int index) {

        this.index = index;
    }
}
