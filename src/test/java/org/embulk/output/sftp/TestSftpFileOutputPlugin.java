/*
 * Copyright 2015 Civitaspo, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.sftp;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import com.jcraft.jsch.JSchException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.scp.ScpCommandFactory;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.OutputPlugin.Control;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.TempFileSpaceImpl;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static com.google.common.io.Files.readLines;
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestSftpFileOutputPlugin
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(new Properties());

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "sftp", SftpFileOutputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    public String oldVfsSftpSshdir = null;
    public String dotSshFolder = null;

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = SftpFileOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = SftpFileOutputPlugin.CONFIG_MAPPER;
    private static final TaskMapper TASK_MAPPER = SftpFileOutputPlugin.TASK_MAPPER;

    private Logger logger = LoggerFactory.getLogger(TestSftpFileOutputPlugin.class);
    private FileOutputRunner runner;
    private SshServer sshServer;
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 20022;
    private static final String PROXY_HOST = "127.0.0.1";
    private static final int PROXY_PORT = 8080;
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String SECRET_KEY_FILE = Resources.getResource("id_rsa").getPath();
    private static final String SECRET_KEY_PASSPHRASE = "SECRET_KEY_PASSPHRASE";
    private static final Schema SCHEMA = new Schema.Builder()
            .add("_c0", BOOLEAN)
            .add("_c1", LONG)
            .add("_c2", DOUBLE)
            .add("_c3", STRING)
            .add("_c4", TIMESTAMP)
            .add("_c5", JSON)
            .build();

    private final String defaultPathPrefix = "/test/testUserPassword";

    @Before
    public void createResources()
            throws IOException
    {
        this.dotSshFolder = testFolder.newFolder().getAbsolutePath();
        this.oldVfsSftpSshdir = System.getProperty("vfs.sftp.sshdir");
        System.setProperty("vfs.sftp.sshdir", this.dotSshFolder);

        // setup the plugin
        SftpFileOutputPlugin sftpFileOutputPlugin = new SftpFileOutputPlugin();
        runner = new FileOutputRunner(sftpFileOutputPlugin);

        sshServer = createSshServer(HOST, PORT, USERNAME, PASSWORD);
    }

    private SshServer createSshServer(String host, int port, final String sshUsername, final String sshPassword)
    {
        // setup a mock sftp server
        SshServer sshServer = SshServer.setUpDefaultServer();
        VirtualFileSystemFactory fsFactory = new VirtualFileSystemFactory();
        fsFactory.setUserHomeDir(sshUsername, testFolder.getRoot().toPath());
        sshServer.setFileSystemFactory(fsFactory);
        sshServer.setHost(host);
        sshServer.setPort(port);
        sshServer.setSubsystemFactories(Collections.<NamedFactory<Command>>singletonList(new SftpSubsystemFactory()));
        sshServer.setCommandFactory(new ScpCommandFactory());
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setPasswordAuthenticator(new PasswordAuthenticator()
        {
            @Override
            public boolean authenticate(final String username, final String password, final ServerSession session)
            {
                return sshUsername.contentEquals(username) && sshPassword.contentEquals(password);
            }
        });
        sshServer.setPublickeyAuthenticator(new PublickeyAuthenticator()
        {
            @Override
            public boolean authenticate(String username, PublicKey key, ServerSession session)
            {
                return true;
            }
        });

        try {
            sshServer.start();
        }
        catch (IOException e) {
            logger.debug(e.getMessage(), e);
        }
        return sshServer;
    }

    private HttpProxyServer createProxyServer(int port)
    {
        return DefaultHttpProxyServer.bootstrap()
                .withPort(port)
                .withFiltersSource(new HttpFiltersSourceAdapter() {
                        public HttpFilters filterRequest(HttpRequest originalRequest, ChannelHandlerContext ctx) {
                            return new HttpFiltersAdapter(originalRequest) {
                                @Override
                                public HttpResponse clientToProxyRequest(final HttpObject httpObject) {
                                    if (httpObject instanceof DefaultHttpRequest) {
                                        final DefaultHttpRequest request = (DefaultHttpRequest) httpObject;
                                        logger.info(
                                                "<HttpProxyServer Filter> clientToProxyRequest DefaultHttpRequest method={} uri={} protocolVersion={} headers={} keep-alive={}",
                                                request.getMethod(),
                                                request.getUri(),
                                                request.getProtocolVersion(),
                                                request.headers().entries(),
                                                HttpHeaders.isKeepAlive(request));
                                    }
                                    else {
                                        logger.info("<HttpProxyServer Filter> clientToProxyRequest {}", httpObject);
                                    }
                                    return null;
                                }

                                @Override
                                public HttpObject proxyToClientResponse(final HttpObject httpObject) {
                                    if (httpObject instanceof DefaultHttpResponse) {
                                        final DefaultHttpResponse response = (DefaultHttpResponse) httpObject;
                                        logger.info(
                                                "<HttpProxyServer Filter> proxyToClientResponse DefaultHttpResponse status={} protocolVersion={} headers={} keep-alive={}",
                                                response.getStatus(),
                                                response.getProtocolVersion(),
                                                response.headers().entries(),
                                                HttpHeaders.isKeepAlive(response));
                                    }
                                    else {
                                        logger.info("<HttpProxyServer Filter> proxyToClientResponse {}", httpObject);
                                    }
                                    return httpObject;
                                }

                                @Override
                                public void proxyToServerConnectionFailed() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerConnectionFailed");
                                }

                                @Override
                                public void proxyToServerConnectionQueued() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerConnectionQueued");
                                }

                                @Override
                                public void proxyToServerConnectionSSLHandshakeStarted() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerConnectionSSLHandshakeStarted");
                                }

                                @Override
                                public void proxyToServerConnectionStarted() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerConnectionStarted");
                                }

                                @Override
                                public void proxyToServerConnectionSucceeded(ChannelHandlerContext serverCtx) {
                                    logger.info("<HttpProxyServer Filter> proxyToServerConnectionSucceeded");
                                }

                                @Override
                                public HttpResponse proxyToServerRequest(HttpObject httpObject) {
                                    if (httpObject instanceof DefaultHttpRequest) {
                                        final DefaultHttpRequest request = (DefaultHttpRequest) httpObject;
                                        logger.info(
                                                "<HttpProxyServer Filter> proxyToServerRequest DefaultHttpRequest method={} uri={} protocolVersion={} headers={} keep-alive={}",
                                                request.getMethod(),
                                                request.getUri(),
                                                request.getProtocolVersion(),
                                                request.headers().entries(),
                                                HttpHeaders.isKeepAlive(request));
                                    }
                                    else {
                                        logger.info("<HttpProxyServer Filter> proxyToServerRequest {}", httpObject);
                                    }
                                    return null;
                                }

                                @Override
                                public void proxyToServerRequestSending() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerRequestSending");
                                }

                                @Override
                                public void proxyToServerRequestSent() {
                                    logger.info("<HttpProxyServer Filter> proxyToServerRequestSent");
                                }

                                @Override
                                public void proxyToServerResolutionFailed(final String hostAndPort) {
                                    logger.info("<HttpProxyServer Filter> proxyToServerResolutionFailed {}", hostAndPort);
                                }

                                @Override
                                public InetSocketAddress proxyToServerResolutionStarted(final String resolvingServerHostAndPort) {
                                    logger.info("<HttpProxyServer Filter> proxyToServerResolutionStarted {}", resolvingServerHostAndPort);
                                    return null;
                                }

                                @Override
                                public void proxyToServerResolutionSucceeded(final String serverHostAndPort, final InetSocketAddress resolvedRemoteAddress) {
                                    logger.info(
                                            "<HttpProxyServer Filter> proxyToServerResolutionSucceeded {} {}",
                                            serverHostAndPort,
                                            resolvedRemoteAddress);
                                }

                                @Override
                                public HttpObject serverToProxyResponse(final HttpObject httpObject) {
                                    if (httpObject instanceof DefaultHttpResponse) {
                                        final DefaultHttpResponse response = (DefaultHttpResponse) httpObject;
                                        logger.info(
                                                "<HttpProxyServer Filter> serverToProxyResponse DefaultHttpResponse status={} protocolVersion={} headers={} keep-alive={}",
                                                response.getStatus(),
                                                response.getProtocolVersion(),
                                                response.headers().entries(),
                                                HttpHeaders.isKeepAlive(response));
                                    }
                                    else {
                                        logger.info("<HttpProxyServer Filter> serverToProxyResponse {}", httpObject);
                                    }
                                    return httpObject;
                                }

                                @Override
                                public void serverToProxyResponseReceived() {
                                    logger.info("<HttpProxyServer Filter> serverToProxyResponseReceived");
                                }

                                @Override
                                public void serverToProxyResponseReceiving() {
                                    logger.info("<HttpProxyServer Filter> serverToProxyResponseReceiving");
                                }

                                @Override
                                public void serverToProxyResponseTimedOut() {
                                    logger.info("<HttpProxyServer Filter> serverToProxyResponseTimedOut");
                                }
                            };
                        }
                    })
                .start();
    }

    @After
    public void cleanup() throws InterruptedException
    {
        try {
            sshServer.stop(true);
        }
        catch (Exception e) {
            logger.debug(e.getMessage(), e);
        }
        finally {
            if (this.oldVfsSftpSshdir == null) {
                System.clearProperty("vfs.sftp.sshdir");
            }
            else {
                System.setProperty("vfs.sftp.sshdir", this.oldVfsSftpSshdir);
            }
        }
    }

    private List<String> lsR(List<String> fileNames, Path dir)
    {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                if (path.toFile().isDirectory()) {
                    lsR(fileNames, path);
                }
                else {
                    fileNames.add(path.toAbsolutePath().toString());
                }
            }
        }
        catch (IOException e) {
            logger.debug(e.getMessage(), e);
        }
        return fileNames;
    }

    private void assertRecordsInFile(String filePath)
    {
        try {
            List<String> lines = readLines(new File(filePath),
                    Charsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String[] record = lines.get(i).split(",");
                if (i == 0) {
                    for (int j = 0; j <= 4; j++) {
                        assertEquals("_c" + j, record[j]);
                    }
                }
                else {
                    // true,2,3.0,45,1970-01-01 00:00:00.678000 +0000
                    assertEquals("true", record[0]);
                    assertEquals("2", record[1]);
                    assertEquals("3.0", record[2]);
                    assertEquals("45", record[3]);
                    assertEquals("1970-01-01 00:00:00.678000 +0000", record[4]);
                    assertEquals("{\"k\":\"v\"}", record[5]);
                }
            }
        }
        catch (IOException e) {
            logger.debug(e.getMessage(), e);
        }
    }

    @Test(expected = ConfigException.class)
    public void testInvalidHost()
    {
        // setting embulk config
        final String pathPrefix = "/test/testUserPassword";

        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST + " ")
                .set("user", USERNAME)
                .set("path_prefix", pathPrefix)
                .set("file_ext", "txt")
                .set("formatter", formatterConfig);

        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        SftpUtils utils = new SftpUtils(task);
        utils.validateHost(task);
    }

    @Test
    public void testConfigValuesIncludingDefault()
    {
        // setting embulk config
        final String pathPrefix = "/test/testUserPassword";

        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("user", USERNAME)
                .set("path_prefix", pathPrefix)
                .set("file_ext", "txt")
                .set("formatter", formatterConfig);

        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        assertEquals(HOST, task.getHost());
        assertEquals(22, task.getPort());
        assertEquals(USERNAME, task.getUser());
        assertEquals(Optional.empty(), task.getPassword());
        assertEquals(Optional.empty(), task.getSecretKeyFilePath());
        assertEquals("", task.getSecretKeyPassphrase());
        assertEquals(true, task.getUserDirIsRoot());
        assertEquals(600, task.getSftpConnectionTimeout());
        assertEquals(5, task.getMaxConnectionRetry());
        assertEquals(pathPrefix, task.getPathPrefix());
        assertEquals("txt", task.getFileNameExtension());
        assertEquals("%03d.%02d.", task.getSequenceFormat());
        assertEquals(Optional.empty(), task.getProxy());
    }

    @Test
    public void testConfigValuesIncludingProxy()
    {
        // setting embulk config
        final String pathPrefix = "/test/testUserPassword";

        final ConfigSource proxyConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "http")
                .set("host", "proxy_host")
                .set("port", "80")
                .set("user", "proxy_user")
                .set("password", "proxy_pass")
                .set("command", "proxy_command");

        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("user", USERNAME)
                .set("path_prefix", pathPrefix)
                .set("file_ext", "txt")
                .set("proxy", proxyConfig)
                .set("formatter", formatterConfig);

        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        ProxyTask proxy = task.getProxy().get();
        assertEquals("proxy_command", proxy.getCommand().get());
        assertEquals("proxy_host", proxy.getHost().get());
        assertEquals("proxy_user", proxy.getUser().get());
        assertEquals("proxy_pass", proxy.getPassword().get());
        assertEquals(80, proxy.getPort());
        assertEquals(ProxyTask.ProxyType.HTTP, proxy.getType());
    }

    // Cases
    //   login(all cases needs host + port)
    //     user + password
    //     user + secret_key_file + secret_key_passphrase
    //   put files
    //     user_directory_is_root
    //     not user_directory_is_root
    //   timeout
    //     0 second

    @Test
    public void testUserPasswordAndPutToUserDirectoryRoot() throws IOException
    {
        // setting embulk config
        final String pathPrefix = "/test/testUserPassword";

        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", pathPrefix)
                .set("file_ext", "txt")
                .set("formatter", formatterConfig);

        this.embulk.runOutput(config, Paths.get(Resources.getResource("in.csv").getPath()));

        List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(testFolder.getRoot().getAbsolutePath()));
        assertThat(fileList, hasItem(containsString(pathPrefix + "000.00.txt")));
        assertRecordsInFile(String.format("%s/%s000.00.txt",
                testFolder.getRoot().getAbsolutePath(),
                pathPrefix));
    }

    @Test
    public void testUserSecretKeyFileAndPutToRootDirectory() throws IOException
    {
        // setting embulk config
        final String pathPrefix = "/test/testUserPassword";

        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", USERNAME)
                .set("secret_key_file", SECRET_KEY_FILE)
                .set("secret_key_passphrase", SECRET_KEY_PASSPHRASE)
                .set("path_prefix", testFolder.getRoot().getAbsolutePath() + pathPrefix)
                .set("file_ext", "txt")
                .set("formatter", formatterConfig);

        this.embulk.runOutput(config, Paths.get(Resources.getResource("in.csv").getPath()));

        List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(testFolder.getRoot().getAbsolutePath()));
        assertThat(fileList, hasItem(containsString(pathPrefix + "000.00.txt")));

        assertRecordsInFile(String.format("%s/%s000.00.txt",
                testFolder.getRoot().getAbsolutePath(),
                pathPrefix));
    }

    @Test
    public void testUserSecretKeyFileWithProxy() throws IOException
    {
        HttpProxyServer proxyServer = null;
        try {
            proxyServer = createProxyServer(PROXY_PORT);

            // setting embulk config
            final String pathPrefix = "/test/testUserPassword";

            final ConfigSource proxyConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                    .set("type", "http")
                    .set("host", PROXY_HOST)
                    .set("port", PROXY_PORT)
                    .set("user", USERNAME)
                    .set("password", PASSWORD)
                    .set("command", "");

            final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                    .set("type", "csv")
                    .set("newline", "CRLF")
                    .set("newline_in_field", "LF")
                    .set("header_line", "true")
                    .set("charset", "UTF-8")
                    .set("quote_policy", "NONE")
                    .set("quote", "\"")
                    .set("escape", "\\")
                    .set("null_string", "")
                    .set("default_timezone", "UTC");

            final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                    .set("type", "sftp")
                    .set("host", HOST)
                    .set("port", PORT)
                    .set("user", USERNAME)
                    .set("secret_key_file", SECRET_KEY_FILE)
                    .set("secret_key_passphrase", SECRET_KEY_PASSPHRASE)
                    .set("path_prefix", testFolder.getRoot().getAbsolutePath() + pathPrefix)
                    .set("file_ext", "txt")
                    .set("proxy", proxyConfig)
                    .set("formatter", formatterConfig);

            this.embulk.runOutput(config, Paths.get(Resources.getResource("in.csv").getPath()));

            List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(testFolder.getRoot().getAbsolutePath()));
            assertThat(fileList, hasItem(containsString(pathPrefix + "000.00.txt")));

            assertRecordsInFile(String.format("%s/%s000.00.txt",
                    testFolder.getRoot().getAbsolutePath(),
                    pathPrefix));
        }
        finally {
            if (proxyServer != null) {
                proxyServer.stop();
            }
        }
    }

    @Test
    public void testProxyType()
    {
        // test valueOf()
        assertEquals("http", ProxyTask.ProxyType.valueOf("HTTP").toString());
        assertEquals("socks", ProxyTask.ProxyType.valueOf("SOCKS").toString());
        assertEquals("stream", ProxyTask.ProxyType.valueOf("STREAM").toString());
        try {
            ProxyTask.ProxyType.valueOf("non-existing-type");
        }
        catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }

        // test fromString
        assertEquals(ProxyTask.ProxyType.HTTP, ProxyTask.ProxyType.fromString("http"));
        assertEquals(ProxyTask.ProxyType.SOCKS, ProxyTask.ProxyType.fromString("socks"));
        assertEquals(ProxyTask.ProxyType.STREAM, ProxyTask.ProxyType.fromString("stream"));
        try {
            ProxyTask.ProxyType.fromString("non-existing-type");
        }
        catch (Exception e) {
            assertEquals(ConfigException.class, e.getClass());
        }
    }

    @Test
    public void testUploadFileWithRetry() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = Mockito.spy(new SftpUtils(task));

        // append throws exception
        Mockito.doThrow(new IOException("Fake Exception"))
                .doCallRealMethod()
                .when(utils)
                .appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));

        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        utils.uploadFile(input, defaultPathPrefix);

        // assert retry and recover
        Mockito.verify(utils, Mockito.times(2)).appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));
        List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(testFolder.getRoot().getAbsolutePath()));
        assertThat(fileList, hasItem(containsString(defaultPathPrefix)));

        // assert uploaded file
        String filePath = testFolder.getRoot().getAbsolutePath() + File.separator + defaultPathPrefix;
        File output = new File(filePath);
        InputStream in = new FileInputStream(output);
        byte[] actual = new byte[8];
        in.read(actual);
        in.close();
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testUploadFileRetryAndGiveUp() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = Mockito.spy(new SftpUtils(task));

        // append throws exception
        Mockito.doThrow(new IOException("Fake IOException"))
                .when(utils)
                .appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));

        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        try {
            utils.uploadFile(input, defaultPathPrefix);
            fail("Should not reach here");
        }
        catch (Exception e) {
            assertThat(e, CoreMatchers.<Exception>instanceOf(RuntimeException.class));
            assertThat(e.getCause(), CoreMatchers.<Throwable>instanceOf(IOException.class));
            assertEquals("Fake IOException", e.getCause().getMessage());
            // assert used up all retries
            Mockito.verify(utils, Mockito.times(task.getMaxConnectionRetry() + 1)).appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));
            assertEmptyUploadedFile(defaultPathPrefix);
        }
    }

    @Test
    public void testUploadFileNotRetryAuthFail() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = Mockito.spy(new SftpUtils(task));

        // append throws exception
        Mockito.doThrow(new IOException(new JSchException("Auth fail")))
                .doCallRealMethod()
                .when(utils)
                .appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));

        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        try {
            utils.uploadFile(input, defaultPathPrefix);
            fail("Should not reach here");
        }
        catch (Exception e) {
            assertThat(e, CoreMatchers.<Exception>instanceOf(RuntimeException.class));
            assertThat(e.getCause(), CoreMatchers.<Throwable>instanceOf(IOException.class));
            assertThat(e.getCause().getCause(), CoreMatchers.<Throwable>instanceOf(JSchException.class));
            assertEquals("Auth fail", e.getCause().getCause().getMessage());
            // assert no retry
            Mockito.verify(utils, Mockito.times(1)).appendFile(Mockito.any(File.class), Mockito.any(FileObject.class), Mockito.any(BufferedOutputStream.class));
            assertEmptyUploadedFile(defaultPathPrefix);
        }
    }

    @Test
    public void testAppendFile() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);

        FileObject remoteFile = utils.resolve(defaultPathPrefix);
        BufferedOutputStream remoteOutput = utils.openStream(remoteFile);
        // 1st append
        byte[] expected = randBytes(16);
        utils.appendFile(writeBytesToInputFile(Arrays.copyOfRange(expected, 0, 8)), remoteFile, remoteOutput);
        // 2nd append
        utils.appendFile(writeBytesToInputFile(Arrays.copyOfRange(expected, 8, 16)), remoteFile, remoteOutput);
        remoteOutput.close();
        remoteFile.close();

        // assert uploaded file
        String filePath = testFolder.getRoot().getAbsolutePath() + File.separator + defaultPathPrefix;
        File output = new File(filePath);
        InputStream in = new FileInputStream(output);
        byte[] actual = new byte[16];
        in.read(actual);
        in.close();
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testAppendFileAndTimeOut() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);
        utils.writeTimeout = 1; // 1s time-out

        byte[] expected = randBytes(8);
        FileObject remoteFile = utils.resolve(defaultPathPrefix);
        BufferedOutputStream remoteOutput = Mockito.spy(utils.openStream(remoteFile));

        Mockito.doAnswer(new Answer()
        {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable
            {
                Thread.sleep(2000); // 2s
                return null;
            }
        }).when(remoteOutput).write(Mockito.any(byte[].class), Mockito.eq(0), Mockito.eq(8));

        try {
            utils.appendFile(writeBytesToInputFile(expected), remoteFile, remoteOutput);
            fail("Should not reach here");
        }
        catch (IOException e) {
            assertThat(e.getCause(), CoreMatchers.<Throwable>instanceOf(TimeoutException.class));
            assertNull(e.getCause().getMessage());
        }
    }

    @Test
    public void testRenameFileWithRetry() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = Mockito.spy(new SftpUtils(task));

        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        utils.uploadFile(input, defaultPathPrefix);

        Mockito.doThrow(new RuntimeException("Fake Exception"))
                .doCallRealMethod()
                .when(utils).resolve(Mockito.eq(defaultPathPrefix));

        utils.renameFile(defaultPathPrefix, "/after");
        Mockito.verify(utils, Mockito.times(1 + 2)).resolve(Mockito.any(String.class)); // 1 fail + 2 success

        // assert renamed file
        String filePath = testFolder.getRoot().getAbsolutePath() + "/after";
        File output = new File(filePath);
        InputStream in = new FileInputStream(output);
        byte[] actual = new byte[8];
        in.read(actual);
        in.close();
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testDeleteFile() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);

        // upload file
        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        utils.uploadFile(input, defaultPathPrefix);

        FileObject target = utils.resolve(defaultPathPrefix);
        assertTrue("File should exists", target.exists());

        utils.deleteFile(defaultPathPrefix);

        target = utils.resolve(defaultPathPrefix);
        assertFalse("File should be deleted", target.exists());
    }

    @Test
    public void testDeleteFileNotExists()
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);
        utils.deleteFile("/not/exists");
    }

    @Test
    public void testResolveWithoutRetry()
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = Mockito.spy(new SftpUtils(task));

        Mockito.doThrow(new ConfigException("Fake ConfigException"))
                .doCallRealMethod()
                .when(utils).getSftpFileUri(Mockito.eq(defaultPathPrefix));

        try {
            utils.resolve(defaultPathPrefix);
            fail("Should not reach here");
        }
        catch (Exception e) {
            assertThat(e, CoreMatchers.<Exception>instanceOf(ConfigException.class));
            // assert retry
            Mockito.verify(utils, Mockito.times(1)).getSftpFileUri(Mockito.eq(defaultPathPrefix));
        }
    }

    @Test
    public void testOpenStreamWithoutRetry() throws FileSystemException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);

        FileObject mock = Mockito.spy(utils.resolve(defaultPathPrefix));
        Mockito.doThrow(new FileSystemException("Fake FileSystemException"))
                .doCallRealMethod()
                .when(mock).getContent();

        try {
            utils.openStream(mock);
            fail("Should not reach here");
        }
        catch (FileSystemException e) {
            Mockito.verify(mock, Mockito.times(1)).getContent();
        }
    }

    @Test
    public void testNewSftpFileExists() throws IOException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);

        byte[] expected = randBytes(8);
        File input = writeBytesToInputFile(expected);
        utils.uploadFile(input, defaultPathPrefix);

        FileObject file = utils.resolve(defaultPathPrefix);
        assertTrue("File should exists", file.exists());

        file = utils.newSftpFile(utils.getSftpFileUri(defaultPathPrefix));
        assertFalse("File should be deleted", file.exists());
    }

    @Test
    public void testNewSftpFileParentNotExists() throws FileSystemException
    {
        SftpFileOutputPlugin.PluginTask task = defaultTask();
        SftpUtils utils = new SftpUtils(task);

        String parentPath = defaultPathPrefix.substring(0, defaultPathPrefix.lastIndexOf('/'));
        FileObject parent = utils.resolve(parentPath);
        boolean exists = parent.exists();
        logger.info("Resolving parent path: {}, exists? {}", parentPath, exists);
        assertFalse("Parent folder should not exist", exists);

        utils.newSftpFile(utils.getSftpFileUri(defaultPathPrefix));

        parent = utils.resolve(parentPath);
        exists = parent.exists();
        logger.info("Resolving (again) parent path: {}, exists? {}", parentPath, exists);
        assertTrue("Parent folder must be created", parent.exists());
    }

    @Test
    public void testSftpFileOutputNextFile() throws IOException
    {
        final TempFileSpace tempFileSpace = TempFileSpaceImpl.with(testFolder.newFolder().toPath(), "output-sftp");

        SftpFileOutputPlugin.PluginTask task = defaultTask();

        SftpLocalFileOutput localFileOutput = new SftpLocalFileOutput(task, 1, tempFileSpace);
        localFileOutput.nextFile();
        assertNotNull("Must use local temp file", localFileOutput.getLocalOutput());
        assertNull("Must not use remote temp file", localFileOutput.getRemoteOutput());
        localFileOutput.close();

        SftpRemoteFileOutput remoteFileOutput = new SftpRemoteFileOutput(task, 1, tempFileSpace);
        remoteFileOutput.nextFile();
        assertNull("Must not use local temp file", remoteFileOutput.getLocalOutput());
        assertNotNull("Must use remote temp file", remoteFileOutput.getRemoteOutput());
        remoteFileOutput.close();
    }

    private ConfigSource defaultConfig(final String pathPrefix)
    {
        final ConfigSource formatterConfig = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "csv")
                .set("newline", "CRLF")
                .set("newline_in_field", "LF")
                .set("header_line", "true")
                .set("charset", "UTF-8")
                .set("quote_policy", "NONE")
                .set("quote", "\"")
                .set("escape", "\\")
                .set("null_string", "")
                .set("default_timezone", "UTC");

        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "sftp")
                .set("host", HOST)
                .set("port", PORT)
                .set("user", USERNAME)
                .set("password", PASSWORD)
                .set("path_prefix", pathPrefix)
                .set("file_ext", "txt")
                .set("formatter", formatterConfig);
    }

    private PluginTask defaultTask()
    {
        ConfigSource config = defaultConfig(defaultPathPrefix);
        return CONFIG_MAPPER.map(config, SftpFileOutputPlugin.PluginTask.class);
    }

    private byte[] randBytes(final int len)
    {
        byte[] bytes = new byte[len];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private File writeBytesToInputFile(final byte[] expected) throws IOException
    {
        File input = File.createTempFile("anything", ".dat");
        OutputStream out = new BufferedOutputStream(new FileOutputStream(input));
        out.write(expected);
        out.close();
        return input;
    }

    private void assertEmptyUploadedFile(final String pathPrefix)
    {
        List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(testFolder.getRoot().getAbsolutePath()));
        assertThat(fileList, hasItem(containsString(pathPrefix)));

        String filePath = testFolder.getRoot().getAbsolutePath() + File.separator + pathPrefix;
        File output = new File(filePath);
        assertEquals(0, output.length());
    }
}
