# Sftp file output plugin for Embulk
[![Build Status](https://travis-ci.org/civitaspo/embulk-output-sftp.svg)](https://travis-ci.org/civitaspo/embulk-output-sftp)
[![Coverage Status](https://coveralls.io/repos/civitaspo/embulk-output-sftp/badge.svg?branch=master&service=github)](https://coveralls.io/github/civitaspo/embulk-output-sftp?branch=master)

Stores files on a SFTP Server

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **host**: (string, required)
- **port**: (int, default: `22`)
- **user**: (string, required)
- **password**: (string, default: `null`)
- **secret_key_file**: (string, default: `null`) [see below](#secret-keyfile-configuration)
- **secret_key_passphrase**: (string, default: `""`)
- **user_directory_is_root**: (boolean, default: `true`)
- **timeout**: sftp connection timeout seconds (integer, default: `600`)
- **path_prefix**: Prefix of output paths (string, required)
- **file_ext**: Extension of output files (string, required)
- **sequence_format**: Format for sequence part of output files (string, default: `".%03d.%02d"`)

### Proxy configuration

- **proxy**:
    - **type**: (string(http | socks | stream), required, default: `null`)
        - **http**: use HTTP Proxy
        - **socks**: use SOCKS Proxy
        - **stream**: Connects to the SFTP server through a remote host reached by SSH
    - **host**: (string, required)
    - **port**: (int, default: `22`)
    - **user**: (string, optional)
    - **password**: (string, optional, default: `null`)
    - **command**: (string, optional)
    
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

With proxy
```yaml
out:
  type: sftp
  host: 127.0.0.1
  port: 22
  user: embulk
  secret_key_file: /Users/embulk/.ssh/id_rsa
  secret_key_passphrase: secret_pass
  user_directory_is_root: false
  timeout: 600
  path_prefix: /data/sftp
  proxy:
    type: http
    host: proxy_host
    port: 8080
    user: proxy_user
    password: proxy_secret_pass
    command:
```

### Secret Keyfile configuration

Please set path of secret_key_file as follows.
```yaml
out:
  type: sftp
  ...
  secret_key_file: /path/to/id_rsa
  ...
```

You can also embed contents of secret_key_file at config.yml.
```yaml
out:
  type: sftp
  ...
  secret_key_file:
    content |
      -----BEGIN RSA PRIVATE KEY-----
      ABCDEFG...
      HIJKLMN...
      OPQRSTU...
      -----END RSA PRIVATE KEY-----
  ...
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
