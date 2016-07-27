package self.aub.study.chapter_3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liujinxin
 * @since 2015-06-07 13:40
 */
public class CityAssignment extends BaseFunction {
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    private static Map<String, double[]> CITIES = new HashMap<>();

    static {
        double[] phl = {39.875365, -75.249524};
        CITIES.put("PHL", phl);
        double[] nyc = {40.71448, -74.00598};
        CITIES.put("NYC", nyc);
        double[] sf = {-31.4250142, -62.0841809};
        CITIES.put("SF", sf);
        double[] la = {-43.05374, -11824307};
        CITIES.put("LA", la);
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.get(0);
        double leastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";
        for (Map.Entry<String, double[]> city : CITIES.entrySet()) {
            double R = 6371;
            double x = (city.getValue()[0] - diagnosis.lng) * Math.cos(city.getValue()[0] + diagnosis.lng / 2);
            double y = (city.getValue()[1] - diagnosis.lat);
            double d = Math.sqrt(x * x + y * y) * R;
            if (d < leastDistance) {
                leastDistance = d;
                closestCity = city.getKey();
            }
        }
        List<Object> values = new ArrayList<>();
        values.add(closestCity);
        LOG.debug("Closest city to lat=[{}], lng=[{}]==[{}],d=[{}]", diagnosis.lat, diagnosis.lng, closestCity, leastDistance);
        tridentCollector.emit(values);
    }
}
