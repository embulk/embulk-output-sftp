package org.embulk.output.sftp;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SftpFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("22")
        int getPort();

        @Config("user")
        String getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("secret_key_file")
        @ConfigDefault("null")
        Optional<LocalFile> getSecretKeyFilePath();
        void setSecretKeyFilePath(Optional<LocalFile> secretKeyFilePath);

        @Config("secret_key_passphrase")
        @ConfigDefault("\"\"")
        String getSecretKeyPassphrase();

        @Config("user_directory_is_root")
        @ConfigDefault("true")
        boolean getUserDirIsRoot();

        @Config("timeout")
        @ConfigDefault("600") // 10 minutes
        int getSftpConnectionTimeout();

        @Config("max_connection_retry")
        @ConfigDefault("5") // 5 times retry to connect sftp server if failed.
        int getMaxConnectionRetry();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d.\"")
        String getSequenceFormat();

        @Config("proxy")
        @ConfigDefault("null")
        Optional<ProxyTask> getProxy();

        @Config("rename_file_after_upload")
        @ConfigDefault("false")
        boolean getRenameFileAfterUpload();

        // if `false`, plugin will use remote file as buffer
        @Config("local_buffering")
        @ConfigDefault("true")
        boolean getLocalBuffering();

        @Min(50L * 1024 * 1024) // 50MiB
        @Max(10L * 1024 * 1024 * 1024) // 10GiB
        @Config("temp_file_threshold")
        @ConfigDefault("5368709120") // 5GiB
        long getTempFileThreshold();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        SftpUtils sftpUtils = null;
        try {
            sftpUtils = new SftpUtils(task);
            sftpUtils.validateHost(task);
        }
        finally {
            if (sftpUtils != null) {
                sftpUtils.close();
            }
        }

        // retryable (idempotent) output:
        // return resume(task.dump(), taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileOutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("sftp output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        if (task.getRenameFileAfterUpload()) {
            SftpUtils sftpUtils = null;
            try {
                sftpUtils = new SftpUtils(task);
                for (TaskReport report : successTaskReports) {
                    List<Map<String, String>> moveFileList = report.get(List.class, "file_list");
                    for (Map<String, String> pairFiles : moveFileList) {
                        String temporaryFileName = pairFiles.get("temporary_filename");
                        String realFileName = pairFiles.get("real_filename");

                        sftpUtils.renameFile(temporaryFileName, realFileName);
                    }
                }
            }
            finally {
                if (sftpUtils != null) {
                    sftpUtils.close();
                }
            }
        }
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        if (task.getLocalBuffering()) {
            return new SftpLocalFileOutput(task, taskIndex);
        }
        return new SftpRemoteFileOutput(task, taskIndex);
    }
}
