package org.embulk.output.sftp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;

import java.util.Locale;
import java.util.Optional;

interface ProxyTask
        extends Task
{
    @Config("type")
    ProxyType getType();

    @Config("host")
    Optional<String> getHost();

    @Config("user")
    @ConfigDefault("null")
    Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    @Config("port")
    @ConfigDefault("22")
    int getPort();

    @Config("command")
    @ConfigDefault("null")
    Optional<String> getCommand();

    enum ProxyType
    {
        HTTP,
        SOCKS,
        STREAM;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static ProxyType fromString(String value)
        {
            switch (value) {
                case "http":
                    return HTTP;
                case "socks":
                    return SOCKS;
                case "stream":
                    return STREAM;
                default:
                    throw new ConfigException(String.format("Unknown proxy type '%s'. Supported proxy types are http, socks, stream", value));
            }
        }

        public static SftpFileSystemConfigBuilder setProxyType(SftpFileSystemConfigBuilder builder, FileSystemOptions fsOptions, ProxyTask.ProxyType type)
        {
            SftpFileSystemConfigBuilder.ProxyType setType = null;
            switch (type) {
                case HTTP:
                    setType = SftpFileSystemConfigBuilder.PROXY_HTTP;
                    break;
                case SOCKS:
                    setType = SftpFileSystemConfigBuilder.PROXY_SOCKS5;
                    break;
                case STREAM:
                    setType = SftpFileSystemConfigBuilder.PROXY_STREAM;
            }
            builder.setProxyType(fsOptions, setType);
            return builder;
        }
    }
}
