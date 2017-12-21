0.1.4 (2017-12-21)
- Fix: Disable remote temporary file rename logic
  - https://github.com/embulk/embulk-output-sftp/pull/41
- Enhance: Upgrade "commons-vfs2", "com.jcraft:jsch" and "commons-io:commons-io"
  - https://github.com/embulk/embulk-output-sftp/pull/42

0.1.3 (2017-11-07)
- Enhance: Create temporary file and rename it after upload completed
  - https://github.com/embulk/embulk-output-sftp/pull/37

0.1.2 (2017-07-11)
- Fix: Increment fileIndex after complete uploadFile
  - https://github.com/embulk/embulk-output-sftp/pull/35

0.1.1 (2017-05-29)
- Fix: Improve retry logic
  - https://github.com/embulk/embulk-output-sftp/pull/34

0.0.9 (2017-03-09)
==================
- Fix: Hide password in the log
  - https://github.com/civitaspo/embulk-output-sftp/pull/25

0.0.8 (2016-09-26)
==================
- Fix: Use second as timetout setting instead of milli second
  - https://github.com/civitaspo/embulk-output-sftp/pull/22
- Fix: Fix CI failure only with Java7
  - https://github.com/civitaspo/embulk-output-sftp/pull/21
- Fix: Format code that were warned by `./gradlew checkstyle` command
  - https://github.com/civitaspo/embulk-output-sftp/pull/23

0.0.7 (2016-03-22)
==================
- Fix: Plugin throws ClassNotFoundException with EmbulkEmbed
  - https://github.com/civitaspo/embulk-output-sftp/pull/19

0.0.6 (2016-03-16)
==================
- Fix: Avoid some connection errors
  - https://github.com/civitaspo/embulk-output-sftp/pull/17

0.0.5 (2016-03-09)
==================
- Add: Support MapReduce executor
  - https://github.com/civitaspo/embulk-output-sftp/pull/8
- Fix: Use ConfigException instead of RuntimeException
  - https://github.com/civitaspo/embulk-output-sftp/pull/9/files
- Fix: Check to exist parent directory before uploading files
  - https://github.com/civitaspo/embulk-output-sftp/pull/13
- Add: Support proxy settings
  - https://github.com/civitaspo/embulk-output-sftp/pull/11
