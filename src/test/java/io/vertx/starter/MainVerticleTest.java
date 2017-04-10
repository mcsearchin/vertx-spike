package io.vertx.starter;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.impl.Deployment;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

  private Vertx vertx;

  @Before
  public void setUp(TestContext tc) {
    vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("db", new JsonObject()
        .put("url", "jdbc:h2:mem:test?shutdown=true")
        .put("driver_class", "org.h2.Driver")
      ));
    vertx.deployVerticle(MainVerticle.class.getName(), options, tc.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  @Test
  public void testThatTheServerIsStarted(TestContext tc) {
    Async async = tc.async();
    vertx.createHttpClient().getNow(8080, "localhost", "/", response -> {
      tc.assertEquals(response.statusCode(), 200);
      response.bodyHandler(body -> {
        tc.assertTrue(body.length() > 0);
        async.complete();
      });
    });
  }

  @Test
  public void givenAPegasusIsCreatedItCanBeRetrievedByItsID(TestContext testContext) {
    Async async = testContext.async();
    HttpClient client = vertx.createHttpClient();
    client.put(8080, "localhost", "/pegasus/jdbc", putResponse -> {
      testContext.assertEquals(putResponse.statusCode(), 200);
      putResponse.bodyHandler(body -> {
        JsonArray id = body.toJsonArray();
        testContext.assertEquals(1, id.size());

        client.get(8080, "localhost", "/pegasus/jdbc/" + id.getInteger(0).toString(), getResponse -> getResponse.bodyHandler(getBody -> {
          testContext.assertTrue(getBody.toString().contains("bob"));
          testContext.assertTrue(getBody.toString().contains("the winged horse"));
          async.complete();
        })).end();
      });
    }).end(new JsonObject().put("name", "bob").put("title", "the winged horse").toString());
  }

}
