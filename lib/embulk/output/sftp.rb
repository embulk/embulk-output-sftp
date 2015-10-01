Embulk::JavaPlugin.register_output(
  "sftp", "org.embulk.output.sftp.SftpFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
