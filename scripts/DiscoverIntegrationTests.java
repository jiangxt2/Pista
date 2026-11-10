import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.scalatest.DoNotDiscover;
import org.scalatest.Suite;
import org.scalatest.tools.DiscoverySuite$;
import org.scalatest.tools.SuiteDiscoveryHelper$;

/** Uses ScalaTest's own discovery and tags without executing a test. */
public class DiscoverIntegrationTests {
  private static final String DOCKER = "com.pista.DockerIT";
  private static final String SUBMITTER = "com.pista.SubmitterIT";
  private static final String EXTERNAL =
      "com.pista.spark.sql.batch.iceberg.SparkSQLSubmitterIcebergRestIT";

  private static String encode(String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String owner(String suite, boolean docker, boolean submitter) {
    if (submitter) return "submitter";
    String pkg = suite.contains(".") ? suite.substring(0, suite.lastIndexOf('.')) : "";
    if (!docker && pkg.equals("com.pista.spark.sql.batch.iceberg")) return "iceberg";
    if (!docker) return "";
    switch (pkg) {
      case "com.pista.spark.sql.clickhouse.meta":
      case "com.pista.spark.sql.connector.clickhouse": return "clickhouse";
      case "com.pista.spark.sql.doris.meta":
      case "com.pista.spark.sql.connector.doris": return "doris";
      case "com.pista.spark.sql.test.util":
      case "com.pista.spark.sql.test.container":
      case "com.pista.spark.sql.batch": return "support";
      default: throw new IllegalStateException("Unassigned Docker IT suite: " + suite);
    }
  }

  private static void collect(Suite suite, String selectedClass, List<String> lines,
      Map<String, Set<String>> selectors) {
    scala.collection.Iterator<String> names = suite.testNames().iterator();
    while (names.hasNext()) {
      String name = names.next();
      scala.Option<scala.collection.immutable.Set<String>> tags = suite.tags().get(name);
      boolean docker = tags.isDefined() && tags.get().contains(DOCKER);
      boolean submitter = tags.isDefined() && tags.get().contains(SUBMITTER);
      String shard = owner(selectedClass, docker, submitter);
      Set<String> actual = new HashSet<>();
      for (String candidate : List.of("clickhouse", "doris", "support")) {
        if (docker && !submitter && selectors.get(candidate).contains(selectedClass)) actual.add(candidate);
      }
      if (!docker && selectors.get("iceberg").contains(selectedClass)) actual.add("iceberg");
      if (submitter) actual.add("submitter");
      Set<String> expected = shard.isEmpty() ? Set.of() : Set.of(shard);
      if (!actual.equals(expected))
        throw new IllegalStateException("Selector mismatch for " + suite.suiteId() + ": " + name + " expected=" + expected + " actual=" + actual);
      if (!shard.isEmpty()) {
        if (tags.isDefined() && tags.get().contains("org.scalatest.Ignore"))
          throw new IllegalStateException("Ignored integration test: " + suite.suiteId() + ": " + name);
        lines.add(shard + "\t" + encode(suite.suiteId()) + "\t" + encode(name));
      }
    }
    scala.collection.Iterator<Suite> nested = suite.nestedSuites().iterator();
    while (nested.hasNext()) collect(nested.next(), selectedClass, lines, selectors);
  }

  public static void main(String[] args) throws Exception {
    Path runpath = Path.of(args[0]);
    Path output = Path.of(args[1]);
    Files.createDirectories(output.getParent());
    List<String> lines = new ArrayList<>();
    if (Files.isDirectory(runpath)) {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      scala.collection.immutable.Set<String> suites = SuiteDiscoveryHelper$.MODULE$.
          discoverSuiteNames(scala.collection.JavaConverters.asScalaBuffer(
              Collections.singletonList(runpath.toString())).toList(), loader, scala.Option.empty());
      Map<String, Set<String>> selectors = new HashMap<>();
      String[] shards = {"clickhouse", "doris", "iceberg", "support"};
      for (int i = 0; i < shards.length; i++) {
        Set<String> selected = new HashSet<>();
        for (String pkg : args[i + 2].split(",")) {
          scala.collection.Iterator<String> matched = DiscoverySuite$.MODULE$.
              nestedSuiteNames(pkg, suites, false).iterator();
          while (matched.hasNext()) selected.add(matched.next());
        }
        selectors.put(shards[i], selected);
      }
      scala.collection.Iterator<String> names = suites.iterator();
      while (names.hasNext()) {
        String name = names.next();
        Suite suite = DiscoverySuite$.MODULE$.getSuiteInstance(name, loader);
        if (suite.getClass().getName().endsWith("DeferredAbortedSuite"))
          throw new IllegalStateException("Cannot instantiate discovered suite: " + name);
        collect(suite, name, lines, selectors);
      }
      try (java.util.stream.Stream<Path> files = Files.walk(runpath)) {
        for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".class"))::iterator) {
          String name = runpath.relativize(file).toString().replace('/', '.').replace('\\', '.');
          name = name.substring(0, name.length() - 6);
          if (name.contains("$") || !name.startsWith("com.pista.")) continue;
          Class<?> cls = loader.loadClass(name);
          if (Suite.class.isAssignableFrom(cls) && cls.isAnnotationPresent(DoNotDiscover.class)) {
            if (!name.equals(EXTERNAL)) throw new IllegalStateException("Unlisted external suite: " + name);
            lines.add("external\t" + encode(name));
          }
        }
      }
    }
    Collections.sort(lines);
    Files.write(output, lines, StandardCharsets.UTF_8);
  }
}
