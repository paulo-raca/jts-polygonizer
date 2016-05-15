import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
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
import com.vividsolutions.jts.util.GeometricShapeFactory;

public class Polygonizer implements Iterable<Polygon> {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final Polygon EMPTY_POLYGON = GEOMETRY_FACTORY.createPolygon(null, null);

    List<Edge> edges = new ArrayList<>();

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
            }
            Vertex v2 = vertexes.get(c2);
            if (v2 == null) {
                v2 = new Vertex(c2);
                vertexes.put(c2, v2);
            }

            Edge edge = new Edge(v1, v2, segment);
            this.edges.add(edge);
            this.edges.add(edge.reverse);
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

        //Sort vertexes by X
        Collections.sort(this.edges, new Comparator<Edge>() {
            @Override
            public int compare(Edge o1, Edge o2) {
                return Double.compare(o1.path.getEnvelopeInternal().getMinX(), o2.path.getEnvelopeInternal().getMinX());
            }
        });
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

        while (ring.size() >= 2 && ring.getFirst().v2 == ring.getLast().v1) {
            deadEnds.add(ring.removeFirst().path);
            ring.removeLast();
        }

        return new Cycle(createPolygon(ring), deadEnds);
    }

    public Iterable<Cycle> cycles() {
        return new Iterable<Polygonizer.Cycle>() {
            @Override
            public Iterator<Cycle> iterator() {
                //Clear visited flags
                for (Edge edge : edges) {
                    edge.visited = false;
                }

                Iterator<Edge> unvisitedEdgeIt = Iterators.filter(edges.iterator(), new Predicate<Edge>() {
                    @Override
                    public boolean apply(Edge edge) {
                        return !edge.visited;
                    }
                });
                Iterator<Cycle> cycleIt = Iterators.transform(unvisitedEdgeIt, new Function<Edge, Cycle>() {
                    @Override
                    public Cycle apply(Edge edge) {
                        Cycle c = visit(edge);
                        return c;
                    }
                });
                return cycleIt;
            }
        };
    }

    //Transforms external shells into holes to internal shells
    public Iterable<Cycle> fix_topology() {
        //Append an Outermost shell
        Iterable<Cycle> cycles = Iterables.concat(
                new Iterable<Cycle>() {
                    public Iterator<Cycle> iterator() {
                        return Iterators.singletonIterator(new Cycle());
                    }
                },
                cycles());

        //Detect collisions
        BoundingBoxMatcher<Cycle> collisions = new BoundingBoxMatcher<>(cycles, new Function<Cycle, Envelope>() {
            @Override
            public Envelope apply(Cycle cycle) {
                return cycle.envelope;
            }
        });
        
        Iterable<Cycle> punchedHoles = Iterables.transform(collisions, new Function<BoundingBoxMatcher<Cycle>.Match, Cycle>() {
            @Override
            public Cycle apply(BoundingBoxMatcher<Cycle>.Match match) {
                if (match.matches == null && !match.value.external) {
                    return match.value;
                }
                if (match.value.external && match.matches != null) {
                    Cycle selectedContainer = null;
                    for (Cycle containerCandidate : match.matches) {
                        if (containerCandidate.external) continue;
                        if (!containerCandidate.envelope.contains(match.value.envelope)) continue;
                        if (containerCandidate.shell!=null) {
                            if (!match.value.shell.isEmpty()) {
                                if (!containerCandidate.shell.contains(match.value.shell.getBoundary())) continue;
                            } else {
                                if (!containerCandidate.shell.contains(match.value.lines.get(0))) continue;
                            }
                        }
                        //if (selectedContainer!=null && selectedContainer.shell!=null && !selectedContainer.shell.contains(containerCandidate.shell.getBoundary())) continue;
                        selectedContainer = containerCandidate;
                    }
                    if (!match.value.shell.isEmpty()) {
                        selectedContainer.holes.add(match.value.shell);
                    }
                    selectedContainer.lines.addAll(match.value.lines);
                }
                return null;
            }
        });
        
        return Iterables.filter(punchedHoles, Cycle.class);
    }

    public Iterable<Polygon> polygons(double streetBuffer, double outerBuffer, double innerBuffer) {
        Iterable<Geometry> geometries = Iterables.transform(fix_topology(), new Function<Cycle, Geometry>() {
            @Override
            public Geometry apply(Cycle cycle) {
                MultiLineString lines = createMultiLineString(cycle.lines);

                if (cycle.shell == null) { //Outer shell
                    if (outerBuffer <= 0) {
                        return EMPTY_POLYGON;
                    }
                    Geometry holes = GEOMETRY_FACTORY.createMultiPolygon(cycle.holes.toArray(new Polygon[cycle.holes.size()]));
                    
                    Geometry outer = holes.buffer(outerBuffer).union(lines.buffer(outerBuffer)).difference(holes);
                    Geometry inner = holes;
                    if (streetBuffer != 0) {
                        inner = inner.buffer(streetBuffer);
                    }
                    if (streetBuffer > 0) {
                        inner = inner.union(lines.buffer(streetBuffer));
                    }
                    return outer.difference(inner);
                    
                } else {
                    Geometry ret = cycle.shell;
                    if (!cycle.holes.isEmpty()) {
                        LinearRing[] rings = new LinearRing[cycle.holes.size()];
                        for (int i=0; i<rings.length; i++) {
                            rings[i] = (LinearRing)cycle.holes.get(i).getExteriorRing();
                        }
                        ret = GEOMETRY_FACTORY.createPolygon((LinearRing)cycle.shell.getExteriorRing(), rings);
                    }       
                    
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
                }
            }
        });
        Iterable<Polygon> polygons = Iterables.concat(Iterables.transform(geometries, new Function<Geometry, Iterable<Polygon>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Iterable<Polygon> apply(Geometry geom) {
                return PolygonExtracter.getPolygons(geom);
            }
        }));
        Iterable<Polygon> nonEmptyPolygons = Iterables.filter(polygons, new Predicate<Polygon>() {
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
        return polygons(streetBuffer, outerBuffer, innerBuffer).iterator();
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
            this.reverse = reverse != null ? reverse : new Edge(v2, v1, (LineString)path.reverse(), this);

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
        public final List<Polygon> holes;
        public final boolean external;
        public final Envelope envelope;

        private Cycle() {
            this.shell = null;
            this.lines = new ArrayList<>();
            this.holes = new ArrayList<>();
            this.external = false;
            this.envelope = new Envelope(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        }

        public Cycle(Polygon shell, List<LineString> lines) {
            this.shell = shell;
            this.lines = lines;
            this.holes = new ArrayList<>();
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

    public static List<LineString> circles(double x, double y, double radius, int levels) {
        List<LineString> ret = new ArrayList<>();

        GeometricShapeFactory factory = new GeometricShapeFactory();
        factory.setCentre(new Coordinate(x, y));
        factory.setSize(radius*2);
        ret.add(factory.createCircle().getExteriorRing());

        if (levels > 1) {
            ret.addAll(circles(x-radius/2, y, radius/4, levels-1));
            ret.addAll(circles(x+radius/2, y, radius/4, levels-1));
            ret.addAll(circles(x, y-radius/2, radius/4, levels-1));
            ret.addAll(circles(x, y+radius/2, radius/4, levels-1));
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        List<LineString> segments = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader("src/street_segments.wkt"))) {

            while (in.ready()) {
                String line = in.readLine();
                segments.add((LineString)new WKTReader().read(line));
            }
        }
        //segments.addAll(circles(0,0,meters(1000),1));
        //segments.add(GEOMETRY_FACTORY.createLineString(new Coordinate[]{new Coordinate(0,meters(-200)), new Coordinate(0,meters(200))}));
        System.out.println(GEOMETRY_FACTORY.createMultiLineString(segments.toArray(new LineString[segments.size()])));

        System.out.println("Data read: " + segments.size() + " segments");
        Stopwatch timer = Stopwatch.createStarted();

        Polygonizer poligonizer = new Polygonizer(segments);
        System.out.println("Graph prepared - " + timer);

        //List<Polygon> polygons = poligonizer.get();
        //List<Polygon> polygons = poligonizer.get(meters(10), 0, 0);
        //List<Polygon> polygons = poligonizer.get(meters(-10), 0, 0);
        //List<Polygon> polygons = poligonizer.get(meters(2), 0, meters(50));
        List<Polygon> polygons = poligonizer.get(meters(10), meters(500), 0);

        System.out.println(polygons.size() + " polygons found - " + timer);

        MultiPolygon allPolygons = GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
        allPolygons = (MultiPolygon) TopologyPreservingSimplifier.simplify(allPolygons,  meters(10));
        System.out.println(allPolygons);
    }
}
