package com.pista.spark.sql.test.container

import com.pista.spark.sql.test.util.TestDataGenerator
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, S3Exception}

import java.net.URI
import java.time.Duration

/** MinIO wrapper without fixed host ports or fixed container names. */
final class PistaMinIOContainer(network: org.testcontainers.containers.Network,
                                resourcePrefix: String)
  extends GenericContainer[PistaMinIOContainer](DockerImageName.parse(
    s"minio/minio:${sys.props.getOrElse("minio.image.tag", "RELEASE.2025-06-13T11-33-47Z")}"))
    with AutoCloseable {

  private val apiPort = 9000
  private val consolePort = 9001
  private val access = "minioadmin"
  private val secret = "test-password"

  withNetwork(network)
  withNetworkAliases(s"$resourcePrefix-minio")
  withLabel("pista.it.managed", "true")
  withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
  withEnv("MINIO_ROOT_USER", access)
  withEnv("MINIO_ROOT_PASSWORD", secret)
  withCommand("server", "/data", "--console-address", ":9001")
  withExposedPorts(apiPort, consolePort)
  waitingFor(Wait.forHttp("/minio/health/ready").forPort(apiPort)
    .withStartupTimeout(Duration.ofSeconds(60)))

  val testBucket: String = TestDataGenerator.uniqueBucketName("pista-it-bucket")

  def endpoint: String = s"http://${getHost}:${getMappedPort(apiPort)}"
  def internalEndpoint: String = s"http://$resourcePrefix-minio:$apiPort"
  def accessKey: String = access
  def secretKey: String = secret

  def captureLogs(): Unit =
    ContainerLogUtils.capture(getDockerClient, getContainerId, s"$resourcePrefix-minio")

  override def start(): Unit = {
    super.start()
    val client = S3Client.builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create(access, secret)))
      .forcePathStyle(true)
      .build()
    try {
      client.createBucket(CreateBucketRequest.builder().bucket(testBucket).build())
    } catch {
      case e: S3Exception if e.statusCode() == 409 =>
        // The run-specific bucket is normally new; keep startup idempotent.
    } finally client.close()
  }
}
