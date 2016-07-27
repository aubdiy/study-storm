package self.aub.study.s05_trident.state.partitioned;

import storm.trident.spout.ISpoutPartition;

/**
 * @author liujinxin
 * @since 2015-07-19 22:39
 */
public class S05PartitionObj implements ISpoutPartition {
    private String name;

    public S05PartitionObj(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return name;
    }
}
