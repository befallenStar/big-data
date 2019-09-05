import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSortByDegreeDESC extends WritableComparator {
    public WeatherSortByDegreeDESC() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        return w1.getYear() == w2.getYear() ? -Integer.compare(w1.getDegree(), w2.getDegree()) : Integer.compare(w1.getYear(), w2.getYear());
    }
}
