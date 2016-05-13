public class Envelope1D implements Comparable<Envelope1D>{
    public final double min, max;
    public Envelope1D(double min, double max) {
        this.min = min;
        this.max = max;
    }
    public boolean intersects(Envelope1D o) {
        return Math.max(min, o.min) <= Math.min(max, o.max);
    }
    public double len() {
        return max-min;
    }
    @Override
    public int compareTo(Envelope1D o) {
        if (min != o.min) {
            return Double.compare(min,  o.min);
        } else {
            return Double.compare(max,  o.max);
        }
    }
}