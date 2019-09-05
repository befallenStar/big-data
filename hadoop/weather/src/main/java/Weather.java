import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements WritableComparable<Weather> {
    private int year;
    private int degree;

    public Weather() {
    }

    public Weather(int year, int degree) {
        this.year = year;
        this.setDegree(degree);
    }

    @Override
    public String toString() {
        return "year=" + year + ", degree=" + getDegree();
    }

    @Override
    public int hashCode() {
        return new Integer(this.year + this.getDegree()).hashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.getDegree());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.setDegree(dataInput.readInt());
    }

    public int compareTo(Weather o) {
        return this.year == o.year ? Integer.compare(this.getDegree(), o.getDegree()) : Integer.compare(this.year, o.year);
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }
}
