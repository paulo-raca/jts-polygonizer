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

    Map<Coordinate, Vertex> vertexes;
    double streetBuffer;
    double outerBuffer;
    double innerBuffer;

    public Polygonizer(List<LineString> segments, double streetBuffer, double outerBuffer, double innerBuffer) {
        this.vertexes = new HashMap<>();
        this.streetBuffer = streetBuffer;
        this.outerBuffer = outerBuffer;
        this.innerBuffer = innerBuffer;

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
    }

    @Override
    public Iterator<Polygon> iterator() {
        //Clear visited flags
        for (Vertex v : vertexes.values()) {
            for (Edge e : v.edges) {
                e.visited = false;
            }
        }

        Iterator<Vertex> vertexesIt = vertexes.values().iterator();
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
        Iterator<Polygon> polygonIt = Iterators.concat(Iterators.transform(unvisitedEdgeIt, new Function<Edge, Iterator<Polygon>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Iterator<Polygon> apply(Edge e) {
                return PolygonExtracter.getPolygons(visit(e)).iterator();
            }
        }));
        Iterator<Polygon> nonEmptyPolygonIt = Iterators.filter(polygonIt, new Predicate<Polygon>() {
            @Override
            public boolean apply(Polygon polygon) {
                return !polygon.isEmpty();
            }
        });
        return nonEmptyPolygonIt;
    } 

    public List<Geometry> get() {
        return Lists.newArrayList(iterator());
    }

    private Geometry visit(Edge firstEdge) {
        LinkedList<Edge> ring = new LinkedList<>();
        List<Edge> deadEnds = new ArrayList<>();
        Edge currentEdge = firstEdge;

        while (true) {
            Vertex currentVertex = currentEdge.v2;
            currentEdge = currentVertex.edges.get((currentEdge.index_v2 + 1) % currentVertex.edges.size());
            if (currentEdge.visited) {
                throw new IllegalStateException("Edge visited twice?! " + currentEdge);
            }
            currentEdge.visited = true;

            if (!ring.isEmpty() && ring.getLast().v1 == currentEdge.v2) {
                ring.removeLast();
                deadEnds.add(currentEdge);
            } else {
                ring.add(currentEdge);
            }

            if (currentEdge == firstEdge) {
                break;
            }
        }

        while (!ring.isEmpty() && ring.getFirst().v2 == ring.getLast().v1) {
            deadEnds.add(ring.removeFirst());
            ring.removeLast();
        }


        Polygon polygon = polygonFromEdges(ring);
        MultiLineString deadEndGeom = geometryFromDeadEnds(deadEnds);

        Geometry ret = polygon;
        if (CGAlgorithms.signedArea(polygon.getExteriorRing().getCoordinates()) > 0) {
            if (streetBuffer != 0) {
                ret = ret.buffer(-streetBuffer);
            }
            if (streetBuffer > 0) {
                ret = ret.difference(deadEndGeom.buffer(streetBuffer));
            }

            if (innerBuffer > 0) {
                ret = ret.difference(ret.buffer(-innerBuffer));
            }
        } else {
            if (outerBuffer > 0) {
                Geometry outer = ret.buffer(outerBuffer);
                outer = outer.union(deadEndGeom.buffer(outerBuffer));

                Geometry inner = ret;
                if (streetBuffer != 0) {
                    inner = inner.buffer(streetBuffer);
                }
                if (streetBuffer > 0) {
                    inner = inner.union(deadEndGeom.buffer(streetBuffer));
                }
                ret = outer.difference(inner);
            } else {
                ret = GEOMETRY_FACTORY.createPolygon(null, null);
            }
        }
        return ret;
    }


    private Polygon polygonFromEdges(List<Edge> ring) {
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

    private MultiLineString geometryFromDeadEnds(List<Edge> deadEnds) {
        List<LineString> segments = new ArrayList<>();
        for (Edge edge : deadEnds) {
            segments.add(edge.path);
        }
        return GEOMETRY_FACTORY.createMultiLineString(segments.toArray(new LineString[segments.size()]));
    }


    private static class Vertex {
        Coordinate coord;
        List<Edge> edges;

        public Vertex(Coordinate center) { 
            this.coord = center;
            this.edges = new ArrayList<>();
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


    private static double meters(double meters) {
        return Math.toDegrees(meters / 6378137.0);
    }

    public static void main(String[] args) throws Exception {
        /*
        Coordinate A = new Coordinate(0, 0);
        Coordinate B = new Coordinate(0, 2);
        Coordinate C = new Coordinate(2, 3);
        Coordinate D = new Coordinate(3, 1);
        Coordinate E = new Coordinate(2, 1);
        Coordinate F = new Coordinate(1, 2);
        Coordinate G = new Coordinate(4, 3);
        Coordinate H = new Coordinate(3, 2);
        Coordinate I = new Coordinate(3, 4);
        Coordinate J = new Coordinate(4, 5);

        LineString[] segments = {
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{A, B}),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{A, D}),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{A, E}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{B, C}),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{B, F}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{C, D}),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{C, F}),
                GEOMETRY_FACTORY.createLineString(new Coordinate[]{C, H}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{D, G}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{E, F}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{G, H}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{H, I}),

                GEOMETRY_FACTORY.createLineString(new Coordinate[]{I, J})
        };
        System.out.println(GEOMETRY_FACTORY.createMultiLineString(segments));

        List<Geometry> polygons = new Poligonizer(Arrays.asList(segments), -0.04, 1).get();
        System.out.println(GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()])));
         */

        //BufferedReader in = new BufferedReader(new InputStreamReader(Poligonizer.class.getResourceAsStream("street_segments.wkt"), "UTF-8"));
        List<LineString> segments = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader("src/lagoa.wkt"))) {

            while (in.ready()) {
                String line = in.readLine();
                segments.add((LineString)new WKTReader().read(line));
            }
        }
        System.out.println(GEOMETRY_FACTORY.createMultiLineString(segments.toArray(new LineString[segments.size()])));

        System.out.println("Data read: " + segments.size() + " segments");
        Stopwatch timer = Stopwatch.createStarted();

        //Polygonizer poligonizer = new Polygonizer(segments, 0, 0, 0);
        Polygonizer poligonizer = new Polygonizer(segments, meters(5), meters(50), 0);
        //Polygonizer poligonizer = new Polygonizer(segments, meters(10), 0, 0);
        //Polygonizer poligonizer = new Polygonizer(segments, -meters(10), 0, 0);
        //Polygonizer poligonizer = new Polygonizer(segments, meters(2), 0, meters(50));
        //Polygonizer poligonizer = new Polygonizer(segments, 0.0001, 0.001, 0.0005);
        System.out.println("Graph prepared - " + timer);

        List<Geometry> polygons = poligonizer.get();
        System.out.println(polygons.size() + " polygons found - " + timer);

        MultiPolygon allPolygons = GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
        allPolygons = (MultiPolygon) TopologyPreservingSimplifier.simplify(allPolygons,  meters(2));
        System.out.println(allPolygons);
    }
}
