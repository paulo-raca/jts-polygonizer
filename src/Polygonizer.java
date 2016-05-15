import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.util.PolygonExtracter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;

public class Polygonizer implements Iterable<Polygon> {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    
    List<Vertex> vertexes = new ArrayList<>();
    
    public Polygonizer(List<LineString> segments) {
        Map<Coordinate, Vertex> vertexes = new HashMap<>();

        //Create a graph
        for (LineString segment : segments) {
            Coordinate c1 = segment.getCoordinateN(0);
            Coordinate c2 = segment.getCoordinateN(segment.getNumPoints()-1);

            Vertex v1 = vertexes.get(c1);
            if (v1 == null) {
                v1 = new Vertex(c1);
                vertexes.put(c1, v1);
                this.vertexes.add(v1);
            }
            Vertex v2 = vertexes.get(c2);
            if (v2 == null) {
                v2 = new Vertex(c2);
                vertexes.put(c2, v2);
                this.vertexes.add(v2);
            }

            Edge edge = new Edge(v1, v2, segment);

            v1.edges.add(edge);
            v2.edges.add(edge.reverse);            
        }

        //Sort edges of every vertex
        for (Vertex v : vertexes.values()) {
            Collections.sort(v.edges);
            for (int i=0; i<v.edges.size(); i++) {
                Edge e = v.edges.get(i);
                e.index_v1 = i;
                e.reverse.index_v2 = i;
            }
        }
        
        //Sort vertexes
        Collections.sort(this.vertexes);
    }

    private Cycle visit(Edge firstEdge) {
        LinkedList<Edge> ring = new LinkedList<>();
        List<LineString> deadEnds = new ArrayList<>();
        Edge currentEdge = firstEdge;

        while (true) {
            Vertex currentVertex = currentEdge.v2;
            currentEdge = currentVertex.edges.get((currentEdge.index_v2 + 1) % currentVertex.edges.size());
            if (currentEdge.visited) {
                throw new IllegalStateException("Edge visited twice?! " + currentEdge);
            }
            currentEdge.visited = true;

            if (!ring.isEmpty() && ring.getLast().v1 == currentEdge.v2) {
                deadEnds.add(ring.removeLast().path);
            } else {
                ring.add(currentEdge);
            }

            if (currentEdge == firstEdge) {
                break;
            }
        }

        while (!ring.isEmpty() && ring.getFirst().v2 == ring.getLast().v1) {
            deadEnds.add(ring.removeFirst().path);
            ring.removeLast();
        }

        return new Cycle(createPolygon(ring), deadEnds);
    }
    
    public Iterator<Cycle> cycles() {
        //Clear visited flags
        for (Vertex v : vertexes) {
            for (Edge e : v.edges) {
                e.visited = false;
            }
        }

        Iterator<Vertex> vertexesIt = vertexes.iterator();
        Iterator<Edge> edgeIt = Iterators.concat(Iterators.transform(vertexesIt, new Function<Vertex, Iterator<Edge>>() {
            @Override
            public Iterator<Edge> apply(Vertex v) {
                return v.edges.iterator();
            }
        }));
        Iterator<Edge> unvisitedEdgeIt = Iterators.filter(edgeIt, new Predicate<Edge>() {
            @Override
            public boolean apply(Edge edge) {
                return !edge.visited;
            }
        });
        Iterator<Cycle> cycleIt = Iterators.transform(unvisitedEdgeIt, new Function<Edge, Cycle>() {
            @Override
            public Cycle apply(Edge edge) {
                return visit(edge);
            }
        });
        return cycleIt;
    }
    

    public Iterator<Polygon> polygons(double streetBuffer, double outerBuffer, double innerBuffer) {
        Iterator<Geometry> geometries = Iterators.transform(cycles(), new Function<Cycle, Geometry>() {
            @Override
            public Geometry apply(Cycle cycle) {
                MultiLineString lines = createMultiLineString(cycle.lines);
                if (!cycle.external) {
                    Geometry ret = cycle.shell;
                    if (streetBuffer != 0) {
                        ret = ret.buffer(-streetBuffer);
                        
                        if (streetBuffer > 0) {
                            ret = ret.difference(lines.buffer(streetBuffer));
                        }
                    }                    
                    if (innerBuffer > 0) { //TODO: Optimize out for small geometries
                        ret = ret.difference(ret.buffer(-innerBuffer));
                    }
                    return ret;
                } else {
                    Geometry outer = cycle.shell;
                    if (outerBuffer > 0) {
                        outer = outer.buffer(outerBuffer).union(lines.buffer(outerBuffer));
                    }
                    outer = outer.union(lines.buffer(outerBuffer));
                    
                    Geometry inner = cycle.shell;
                    if (streetBuffer != 0) {
                        inner = inner.buffer(streetBuffer);
                        
                        if (streetBuffer > 0) {
                            inner = inner.union(lines.buffer(streetBuffer));
                        }
                    }     
                    return outer.difference(inner);
                }
            }
        });
        Iterator<Polygon> polygons = Iterators.concat(Iterators.transform(geometries, new Function<Geometry, Iterator<Polygon>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Iterator<Polygon> apply(Geometry geom) {
                return PolygonExtracter.getPolygons(geom).iterator();
            }
        }));
        Iterator<Polygon> nonEmptyPolygons = Iterators.filter(polygons, new Predicate<Polygon>() {
            @Override
            public boolean apply(Polygon polygon) {
                return !polygon.isEmpty();
            }
        });
        return nonEmptyPolygons;
    }
    

    @Override
    public Iterator<Polygon> iterator() {
        return iterator(0, 0, 0);
    }

    public Iterator<Polygon> iterator(double streetBuffer, double outerBuffer, double innerBuffer) {
        return polygons(streetBuffer, outerBuffer, innerBuffer);
    }

    public List<Polygon> get() {
        return get(0,0,0);
    }
    
    public List<Polygon> get(double streetBuffer, double outerBuffer, double innerBuffer) {
        return Lists.newArrayList(polygons(streetBuffer, outerBuffer, innerBuffer));
    }


    private Polygon createPolygon(List<Edge> ring) {
        if (ring.isEmpty()) {
            return GEOMETRY_FACTORY.createPolygon(null, null); 
        }

        List<Coordinate> coords = new ArrayList<>();
        for (Edge edge : ring) {
            for (int i=0; i<edge.path.getNumPoints()-1; i++) {
                coords.add(edge.path.getCoordinateN(i));
            }
        }
        coords.add(coords.get(0));
        LinearRing linearRing = GEOMETRY_FACTORY.createLinearRing(coords.toArray(new Coordinate[coords.size()]));
        return GEOMETRY_FACTORY.createPolygon(linearRing, new LinearRing[0]);
    }

    private MultiLineString createMultiLineString(List<LineString> lines) {
        return GEOMETRY_FACTORY.createMultiLineString(lines.toArray(new LineString[lines.size()]));
    }


    private static class Vertex implements Comparable<Vertex>{
        Coordinate coord;
        List<Edge> edges;

        public Vertex(Coordinate center) { 
            this.coord = center;
            this.edges = new ArrayList<>();
        }
        @Override
        public int compareTo(Vertex o) {
            return Double.compare(this.coord.x, o.coord.x);
        }
        @Override
        public String toString() {
            return "(" + coord.x + "," + coord.y + ")";
        }
    }

    private static class Edge implements Comparable<Edge>{
        Vertex v1;
        Vertex v2;
        int index_v1, index_v2;
        final double sortValue;
        final LineString path;
        final Edge reverse;
        boolean visited;

        public Edge(Vertex v1, Vertex v2, LineString path) {
            this(v1, v2, path, null);
        }

        private Edge(Vertex v1, Vertex v2, LineString path, Edge reverse) {
            this.v1 = v1;
            this.v2 = v2;
            this.path = path;
            this.visited = false;
            this.reverse = reverse != null ? reverse : new Edge(v2, v1, path.reverse(), this);

            Coordinate c1 = path.getCoordinateN(0);
            Coordinate c2 = path.getCoordinateN(1);
            this.sortValue = Math.atan2(c2.y-c1.y, c2.x-c1.x);
        }

        @Override
        public int compareTo(Edge o) {
            return Double.compare(this.sortValue, o.sortValue);
        }

        @Override
        public String toString() {
            return v1 + "→" + v2;
        }
    }

    public static class Cycle {
        public final Polygon shell;
        public final List<LineString> lines;
        public final boolean external;
        public final Envelope envelope;

        public Cycle(Polygon shell, List<LineString> lines) {
            this.shell = shell;
            this.lines = lines;
            this.external = CGAlgorithms.signedArea(shell.getExteriorRing().getCoordinates()) <= 0;
            this.envelope = new Envelope(shell.getEnvelopeInternal());
            if (external) {
                for (LineString line : lines) {
                    this.envelope.expandToInclude(line.getEnvelopeInternal());
                }
            }
        }
    }


    private static double meters(double meters) {
        return Math.toDegrees(meters / 6378137.0);
    }

    public static void main(String[] args) throws Exception {
        List<LineString> segments = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader("src/street_segments.wkt"))) {

            while (in.ready()) {
                String line = in.readLine();
                segments.add((LineString)new WKTReader().read(line));
            }
        }
        System.out.println(GEOMETRY_FACTORY.createMultiLineString(segments.toArray(new LineString[segments.size()])));

        System.out.println("Data read: " + segments.size() + " segments");
        Stopwatch timer = Stopwatch.createStarted();

        Polygonizer poligonizer = new Polygonizer(segments);
        System.out.println("Graph prepared - " + timer);

        //List<Polygon> polygons = poligonizer.get();
        //List<Polygon> polygons = poligonizer.get(meters(10), 0, 0);
        //List<Polygon> polygons = poligonizer.get(meters(-10), 0, 0);
        //List<Polygon> polygons = poligonizer.get(meters(2), 0, meters(50));
        List<Polygon> polygons = poligonizer.get(meters(5), meters(50), meters(100));

        System.out.println(polygons.size() + " polygons found - " + timer);

        MultiPolygon allPolygons = GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
        allPolygons = (MultiPolygon) TopologyPreservingSimplifier.simplify(allPolygons,  meters(5));
        System.out.println(allPolygons);
    }
}
