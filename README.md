# Sftp file output plugin for Embulk

Stores files on a SFTP Server

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **host**: (string, required)
- **port**: (string, default: `22`)
- **user**: (string, required)
- **password**: (string, default: `null`)
- **secret_key_file**: (string, default: `null`)
- **secret_key_passphrase**: (string, default: `""`)
- **user_directory_is_root**: (boolean, default: `true`)
- **timeout**: sftp connection timeout seconds (integer, default: `600`)
- **path_prefix**: Prefix of output paths (string, required)
- **file_ext**: Extension of output files (string, required)
- **sequence_format**: Format for sequence part of output files (string, default: `".%03d.%02d"`)

## Example

```yaml
out:
  type: sftp
  host: 127.0.0.1
  port: 22
  user: civitaspo
  secret_key_file: /Users/civitaspo/.ssh/id_rsa
  secret_key_passphrase: secret_pass
  user_directory_is_root: false
  timeout: 600
  path_prefix: /data/sftp
  file_ext: _20151020.tsv
  sequence_format: ".%01d%01d"
```

## Run Example
replace settings in `example/sample.yml` before running.

```
$ ./gradlew classpath
$ embulk run -Ilib example/sample.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Note

This plugin uses "org.apache.commons:commons-vfs" and the library uses the logger "org.apache.commons.logging.Log". So, this plugin suppress the logger's message except when embulk log level is debug.
