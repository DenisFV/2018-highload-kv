package ru.mail.polis.Denis;

import one.nio.http.*;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;

public class MyService extends HttpServer implements KVService {

    private final KVDao dao;

    public MyService(HttpServerConfig config, KVDao dao, Object... routers) throws IOException {
        super(config, routers);
        this.dao = dao;
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
    public Response entity(@Param("id") String id, Request request) {
        if (id == null || id.isEmpty())
            return new Response(Response.BAD_REQUEST, Response.EMPTY);

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: return get(id);
                case Request.METHOD_PUT: return put(id, request.getBody());
                case Request.METHOD_DELETE: return delete(id);
                default: return null;
            }
        } catch (Throwable e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response get(String id) throws IOException {
        return new Response(Response.OK, dao.get(id.getBytes()));
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
