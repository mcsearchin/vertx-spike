package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Arrays;
import java.util.List;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start() {
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);

    router.route("/").handler(context -> {
      context.response().end("Hello Vert.x!");
    });
    JDBCClient jdbcClient = createJdbcClient();
    initializeDatabase(jdbcClient);

    router.route().handler(BodyHandler.create());
    createPegasusJdbcGetRoute(router, jdbcClient);
    createPegasusJdbcPutRoute(router, jdbcClient);

    server.requestHandler(router::accept).listen(8080);
  }

  private JDBCClient createJdbcClient() {
    JsonObject config;
    if (config().getJsonObject("db") != null) {
      config = config().getJsonObject("db");
    } else {
      config = new JsonObject()
        .put("driver_class", "com.mysql.cj.jdbc.Driver")
        .put("url", "jdbc:mysql://localhost:3306/pegasi?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
        .put("user", "root")
        .put("password", "root");
    }
    return JDBCClient.createShared(vertx, config);
  }

  private void initializeDatabase(JDBCClient client) {
    client.getConnection(connectionProvider -> {
      if (connectionProvider.succeeded()) {
//        connectionProvider.result().execute(
//          "DROP TABLE IF EXISTS pegasus",
//          executionResult -> {
//            System.out.println("Deleted pegasus table : " + (executionResult.succeeded() ? "Success" : "Failed"));
//          });
        connectionProvider.result().execute(
          "CREATE TABLE IF NOT EXISTS pegasus (id integer not null auto_increment, name varchar(255), title varchar(255), primary key (id));",
          executionResult -> {
            System.out.println("Created pegasus table : " + (executionResult.succeeded() ? "Success" : "Failed"));
        });
      } else {
        System.out.println("Failed to get a database connection");
      }
    });
  }

  private Route createPegasusJdbcGetRoute(Router router, JDBCClient jdbcClient) {
    return router.get("/pegasus/jdbc/:id").handler(context -> {
      String id = context.pathParam("id");
      HttpServerResponse response = context.response();
      jdbcClient.getConnection(connectionProvider -> {
        if (connectionProvider.succeeded()) {
          connectionProvider.result().queryWithParams(
            "select id, name, title from pegasus where id = ?",
            new JsonArray(Arrays.asList(id)),
            queryResult -> {
              if (queryResult.succeeded()) {
                List<JsonObject> rows = queryResult.result().getRows();
                if (!rows.isEmpty()) {
                  JsonObject row = rows.iterator().next();

                  response.end(row.getString("name") + " " + row.getString("title"));
                } else {
                  response.end("No pegasus for " + id);
                }
              } else {
                response.end("Query failed");
              }
              queryResult.result();
            }
          );
        } else {
          response.end("Failed to get a database connection");
        }
      });
    });
  }

  private Route createPegasusJdbcPutRoute(Router router, JDBCClient jdbcClient) {
    return router.put("/pegasus/jdbc").handler(context -> {
      JsonObject requestBody = context.getBodyAsJson();
      HttpServerResponse response = context.response();

      jdbcClient.getConnection(connectionProvider -> {
        if (connectionProvider.succeeded()) {
          connectionProvider.result().updateWithParams("insert into pegasus (name, title) value(?, ?)",
            new JsonArray(Arrays.asList(requestBody.getString("name"), requestBody.getString("title"))),
            queryResult -> {
              if (queryResult.succeeded()) {
                response.end(queryResult.result().getKeys().toString());
              } else {
                response.end("Insert failed");
              }
            }
          );
        } else {
          response.end("Failed to get a database connection");
        }
      });
    });
  }
}
