/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * This file is a modified copy from the Apache Commons VFS2.
 *
 * We can remove this file when Apache Commons VFS2 removes permission check logic when remote file renaming.
 * https://github.com/embulk/embulk-output-sftp/issues/40
 * https://github.com/embulk/embulk-output-sftp/pull/44
 * https://issues.apache.org/jira/browse/VFS-590
 */
package org.embulk.output.sftp.provider.sftp;

import com.jcraft.jsch.Session;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.GenericFileName;

/**
 * Represents the files on an SFTP server.
 */
public class SftpFileSystem extends org.apache.commons.vfs2.provider.sftp.SftpFileSystem
{
    protected SftpFileSystem(final GenericFileName rootName, final Session session,
                             final FileSystemOptions fileSystemOptions)
    {
        super(rootName, null, fileSystemOptions);
    }

    /**
     * Creates a file object. This method is called only if the requested file is not cached.
     */
    @Override
    protected FileObject createFile(final AbstractFileName name) throws FileSystemException
    {
        return new SftpFileObject(name, this);
    }
}
