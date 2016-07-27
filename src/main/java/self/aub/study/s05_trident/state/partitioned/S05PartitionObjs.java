package self.aub.study.s05_trident.state.partitioned;


import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * @author liujinxin
 * @since 2015-07-19 22:49
 */
public class S05PartitionObjs implements Iterable<S05PartitionObj>, Serializable {
    private List<S05PartitionObj> list;

    public S05PartitionObjs(List<S05PartitionObj> list) {
        this.list = list;
    }

    @Override
    public Iterator<S05PartitionObj> iterator() {
        return list.iterator();
    }

    public List<S05PartitionObj> getList() {
        return list;
    }
}
