package self.aub.study.chapter_3;

import java.io.Serializable;

/**
 * @author liujinxin
 * @since 2015-06-07 13:17
 */
public class DiagnosisEvent implements Serializable {
    private  static final long serialVersionUID=1l;
    public double lat;
    public double lng;
    public long time;
    public String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        this.lat = lat;
        this.lng = lng;
        this.time = time;
        this.diagnosisCode = diagnosisCode;
    }
}
