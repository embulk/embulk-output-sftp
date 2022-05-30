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

import org.apache.bval.jsr303.ApacheValidationProvider;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.LocalFile;

import javax.validation.Validation;
import javax.validation.Validator;
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
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
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
        control.run(task.toTaskSource());
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
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
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

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
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        if (task.getLocalBuffering()) {
            return new SftpLocalFileOutput(task, taskIndex, Exec.getTempFileSpace());
        }
        return new SftpRemoteFileOutput(task, taskIndex, Exec.getTempFileSpace());
    }

    private static final Validator VALIDATOR =
            Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();
    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().withValidator(VALIDATOR).build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();
}
