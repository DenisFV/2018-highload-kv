package ru.mail.polis.Denis;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.*;

public class MyService extends HttpServer implements KVService {

    private final KVDao dao;
    private final String[] topology;
    private final Map<String, HttpClient> nodes = new TreeMap<>();
    private final String hostport;
    private final ValueSerializer serializer;

    public MyService(HttpServerConfig config, KVDao dao, Set<String> topology, Object... routers) throws IOException {
        super(config, routers);
        this.dao = dao;
        this.topology = topology.toArray(new String[0]);
        hostport = "http://localhost:" + config.acceptors[0].port;
        for (String t : topology)
            nodes.put(t, new HttpClient(new ConnectionString(t)));
        serializer = ValueSerializer.INSTANCE;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }


    @Path("/v0/entity")
    public Response entity(@Param("id") String id, Request request, @Param("replicas") String replicas) {
        boolean proxied = request.getHeader("Proxied") != null;
        int from = topology.length;
        int ack = topology.length / 2 + 1;
        try {
            if (id == null || id.isEmpty()) throw new IllegalArgumentException();
            if (replicas != null) {
                String[] rep = replicas.split("/");
                ack = Integer.parseInt(rep[0]);
                from = Integer.parseInt(rep[1]);
                if (rep.length != 2 || ack <= 0 || ack > from) throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return proxied ? get(id) : proxyGet(id, ack, getNodes(id, from));
                case Request.METHOD_PUT:
                    if (proxied) return put(id, request.getBody());
                    proxyPut(id, ack, getNodes(id, from), serializer.serialize(new Value(request.getBody())));
                    return new Response(Response.CREATED, Response.EMPTY);
                case Request.METHOD_DELETE:
                    if (proxied) return delete(id);
                    proxyPut(id, ack, getNodes(id, from), serializer.serialize(new Value()));
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                default: return null;
            }
        } catch (Throwable e) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response proxyGet(String id, int ack, List<String> from) {
        List<Value> values = new ArrayList<>();
        for (String node : from) {
            if (node.equals(hostport)) {
                try {
                    values.add(serializer.deserialize(dao.get(id.getBytes())));
                } catch (Throwable e) {
                    values.add(Value.UNKNOWN);
                }
            } else {
                try {
                    final Response response = nodes.get(node).get("/v0/entity?id=" + id, "Proxied: true");
                    switch (response.getStatus()) {
                        case 404: values.add(Value.UNKNOWN);
                        case 200: values.add(new Value(
                                response.getBody(),
                                Long.parseLong(response.getHeader("Timestamp")),
                                Value.stateCode.valueOf(response.getHeader("State"))));
                    }
                } catch (Exception e) { }
            }
        }

        if (values.size() >= ack) {
            Value max = values.stream().max(Comparator.comparingLong(Value::getTimestamp)).orElse(Value.UNKNOWN);
            switch (max.getState()) {
                case UNKNOWN:
                case DELETED: return new Response(Response.NOT_FOUND, Response.EMPTY);
                case PRESENT: return new Response(Response.OK, max.getData());
            }
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response get(String id) throws IOException {
        Value value = serializer.deserialize(dao.get(id.getBytes()));
        Response response = new Response(Response.OK, value.getData());
        response.addHeader("Timestamp" + value.getTimestamp());
        response.addHeader("State" + value.getState());
        return response;
    }

    private List<String> getNodes(String key, int length) {
        final List<String> clients = new ArrayList<>();
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        clients.add(topology[firstNodeId]);
        for (int i = 1; i < length; i++)
            clients.add(topology[(firstNodeId + i) % topology.length]);
        return clients;
    }

    private void proxyPut(String id, int ack, List<String> from, byte[] value) throws Exception {
        int fl = 0;
        for (String node : from) {
            try{
                if (node.equals(hostport)) {
                    dao.upsert(id.getBytes(), value);
                    fl++;
                } else if (nodes.get(node).put("/v0/entity?id=" + id, value, "Proxied: true")
                        .getStatus() != 500) fl++;
            } catch (Exception e) { }
        }
        if (fl < ack) throw new Exception();
    }

    private Response put(String id, byte[] value) throws IOException {
        dao.upsert(id.getBytes(), value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) throws IOException {
        dao.remove(id.getBytes());
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
