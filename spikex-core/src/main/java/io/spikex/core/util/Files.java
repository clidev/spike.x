/**
 *
 * Copyright (c) 2015 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package io.spikex.core.util;

import com.google.common.base.Preconditions;
import io.spikex.core.util.process.ChildProcess;
import io.spikex.core.util.process.ProcessExecutor;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File and directory helper methods for Windows and Unix.
 * <p>
 * Ref. http://docs.joomla.org/How_do_Windows_file_permissions_work%3F
 * <p>
 * @author cli
 */
public final class Files {

    public enum Permission {

        OWNER_FULL,
        OWNER_FULL_GROUP_EXEC,
        OWNER_FULL_GROUP_EXEC_OTHER_EXEC
    }

    private static final Set<PosixFilePermission> POSIX_OWNrwx;
    private static final Set<PosixFilePermission> POSIX_OWNrwx_GRPr_x;
    private static final Set<PosixFilePermission> POSIX_OWNrwx_GRPr_x_OTHr_x;

    private static final Set<AclEntryPermission> ACL_FULL;
    private static final Set<AclEntryPermission> ACL_MODIFY;
    private static final Set<AclEntryPermission> ACL_READ_EXEC;

    private static final String WIN_PRINCIPAL_ADMINS = "BUILTIN\\Administrators";
    private static final String WIN_PRINCIPAL_AUTH_USERS = "NT AUTHORITY\\Authenticated Users";

    private static final int HASH_SALT = 0x26762313;

    static {
        //
        POSIX_OWNrwx = EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE);
        //
        POSIX_OWNrwx_GRPr_x = EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_EXECUTE);
        //
        POSIX_OWNrwx_GRPr_x_OTHr_x = EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ,
                PosixFilePermission.OTHERS_EXECUTE);
        //
        ACL_FULL = EnumSet.of(
                AclEntryPermission.ADD_FILE,
                AclEntryPermission.ADD_SUBDIRECTORY,
                AclEntryPermission.APPEND_DATA,
                AclEntryPermission.DELETE,
                AclEntryPermission.DELETE_CHILD,
                AclEntryPermission.EXECUTE,
                AclEntryPermission.LIST_DIRECTORY,
                AclEntryPermission.READ_ACL,
                AclEntryPermission.READ_ATTRIBUTES,
                AclEntryPermission.READ_DATA,
                AclEntryPermission.READ_NAMED_ATTRS,
                AclEntryPermission.SYNCHRONIZE,
                AclEntryPermission.WRITE_ACL,
                AclEntryPermission.WRITE_ATTRIBUTES,
                AclEntryPermission.WRITE_DATA,
                AclEntryPermission.WRITE_NAMED_ATTRS,
                AclEntryPermission.WRITE_OWNER);
        //
        ACL_MODIFY = EnumSet.of(
                AclEntryPermission.ADD_FILE,
                AclEntryPermission.ADD_SUBDIRECTORY,
                AclEntryPermission.APPEND_DATA,
                AclEntryPermission.DELETE,
                AclEntryPermission.DELETE_CHILD,
                AclEntryPermission.EXECUTE,
                AclEntryPermission.LIST_DIRECTORY,
                AclEntryPermission.READ_ACL,
                AclEntryPermission.READ_ATTRIBUTES,
                AclEntryPermission.READ_DATA,
                AclEntryPermission.READ_NAMED_ATTRS,
                AclEntryPermission.SYNCHRONIZE,
                AclEntryPermission.WRITE_ACL,
                AclEntryPermission.WRITE_ATTRIBUTES,
                AclEntryPermission.WRITE_DATA,
                AclEntryPermission.WRITE_NAMED_ATTRS,
                AclEntryPermission.WRITE_OWNER);
        //
        ACL_READ_EXEC = EnumSet.of(
                AclEntryPermission.EXECUTE,
                AclEntryPermission.LIST_DIRECTORY,
                AclEntryPermission.READ_ACL,
                AclEntryPermission.READ_ATTRIBUTES,
                AclEntryPermission.READ_DATA,
                AclEntryPermission.READ_NAMED_ATTRS,
                AclEntryPermission.SYNCHRONIZE);
    }

    private static final String DEF_GROUP_STAFF = "staff";
    private static final String DEF_GROUP_USERS = "users";

    private static final Logger m_logger = LoggerFactory.getLogger(Files.class);

    public static void setOwner(
            final String owner,
            final Path... dirs) throws IOException {

        UserPrincipal principal = FileSystems.getDefault()
                .getUserPrincipalLookupService()
                .lookupPrincipalByName(owner);

        for (Path dir : dirs) {
            try {
                java.nio.file.Files.setOwner(dir, principal);
            } catch (IOException e) {
                m_logger.warn("Failed to set \"{}\" as the owner of \"{}\"",
                        owner, dir);
            }
        }
    }

    public static void setUnixGroup(
            final String group,
            final Path... dirs) throws IOException {

        GroupPrincipal principal;

        try {
            principal = FileSystems.getDefault()
                    .getUserPrincipalLookupService()
                    .lookupPrincipalByGroupName(group);

        } catch (IOException e) {
            //
            // User users in Linux as group
            //
            if (HostOs.isLinux()) {
                principal = FileSystems.getDefault()
                        .getUserPrincipalLookupService()
                        .lookupPrincipalByGroupName(DEF_GROUP_USERS);
            } //
            // Use staff in OS X, BSD, Solaris and others as group
            //
            else {
                principal = FileSystems.getDefault()
                        .getUserPrincipalLookupService()
                        .lookupPrincipalByGroupName(DEF_GROUP_STAFF);
            }
        }

        for (Path dir : dirs) {
            try {
                java.nio.file.Files.getFileAttributeView(
                        dir,
                        PosixFileAttributeView.class,
                        LinkOption.NOFOLLOW_LINKS)
                        .setGroup(principal);
            } catch (IOException e) {
                m_logger.warn("Failed to set \"{}\" as the group of \"{}\"",
                        group, dir);
            }
        }
    }

    public static void createUnixDirectories(
            final Permission perm,
            final Path... dirs) throws IOException {

        FileAttribute<Set<PosixFilePermission>> attr
                = PosixFilePermissions.asFileAttribute(POSIX_OWNrwx);

        switch (perm) {
            case OWNER_FULL_GROUP_EXEC:
                attr = PosixFilePermissions.asFileAttribute(POSIX_OWNrwx_GRPr_x);
                break;
            case OWNER_FULL_GROUP_EXEC_OTHER_EXEC:
                attr = PosixFilePermissions.asFileAttribute(POSIX_OWNrwx_GRPr_x_OTHr_x);
                break;
        }

        for (Path dir : dirs) {
            if (!java.nio.file.Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
                m_logger.debug("Creating directory: {} with permissions: {}", dir, perm);
                java.nio.file.Files.createDirectories(dir, attr);
            } else {
                m_logger.debug("Directory exists: {}", dir);
            }
        }
    }

    public static void createWindowsDirectories(
            final String owner,
            final Path... dirs) throws IOException {

        UserPrincipal principal = null;
        try {
            // Lookup owner
            principal = FileSystems.getDefault()
                    .getUserPrincipalLookupService()
                    .lookupPrincipalByName(owner);

        } catch (IOException e) {
            m_logger.warn("Failed to lookup user: {}", owner);
        }

        for (Path dir : dirs) {

            if (!java.nio.file.Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
                m_logger.debug("Creating directory: {}", dir);
                Path dirPath = java.nio.file.Files.createDirectories(dir); // Create first

                if (principal != null) {
                    AclFileAttributeView view = java.nio.file.Files.getFileAttributeView(
                            dirPath,
                            AclFileAttributeView.class,
                            LinkOption.NOFOLLOW_LINKS);

                    // Owner permissions
                    List<AclEntry> entries = new ArrayList();
                    entries.add(AclEntry.newBuilder()
                            .setType(AclEntryType.ALLOW)
                            .setPermissions(ACL_FULL)
                            .setPrincipal(principal)
                            .build());

                    try {
                        // Admins
                        principal = FileSystems.getDefault()
                                .getUserPrincipalLookupService()
                                .lookupPrincipalByName(WIN_PRINCIPAL_ADMINS);
                        entries.add(AclEntry.newBuilder()
                                .setType(AclEntryType.ALLOW)
                                .setPermissions(ACL_FULL)
                                .setPrincipal(principal)
                                .build());
                    } catch (IOException e) {
                        m_logger.warn("Failed to set permissions for {}: {}",
                                WIN_PRINCIPAL_ADMINS, e.getMessage());
                    }

                    try {
                        // Authenticated users
                        principal = FileSystems.getDefault()
                                .getUserPrincipalLookupService()
                                .lookupPrincipalByName(WIN_PRINCIPAL_AUTH_USERS);
                        entries.add(AclEntry.newBuilder()
                                .setType(AclEntryType.ALLOW)
                                .setPermissions(ACL_FULL)
                                .setPrincipal(principal)
                                .build());
                    } catch (IOException e) {
                        m_logger.warn("Failed to set permissions for {}: {}",
                                WIN_PRINCIPAL_AUTH_USERS, e.getMessage());
                    }

                    // Set permissions
                    m_logger.debug("Setting directory permissions: {}", entries);
                    view.setAcl(entries);
                }
            } else {
                m_logger.debug("Directory exists: {}", dir);
            }
        }
    }

    public static void createDirectories(
            final String owner,
            final Permission perm,
            final Path... dirs) throws IOException {

        if (HostOs.isUnix()) {
            Files.createUnixDirectories(
                    perm,
                    dirs);
        } else if (HostOs.isWindows()) {
            Files.createWindowsDirectories(
                    owner,
                    dirs);
        } else {
            throw new IllegalStateException("Unsupported operating system: "
                    + HostOs.operatingSystem());
        }
    }

    public static Path createSymbolicLink(
            final Path target,
            final Path link) throws IOException {

        m_logger.debug("Removing existing link: {}", link);
        java.nio.file.Files.deleteIfExists(link);

        if (HostOs.isUnix()) {
            m_logger.debug("Creating link: {} to target: {}", link, target);
            java.nio.file.Files.createSymbolicLink(link, target);

        } else if (HostOs.isWindows()) {

            Path targetWin = target;
            if (!targetWin.isAbsolute()) {
                targetWin = link.getParent().resolve(target);
            }

            try {

                // We can use mklink on Vista, Windows 7, etc..
                m_logger.debug("Calling mklink to create link: {} to target: {}",
                        link, targetWin);

                ChildProcess cmd = new ProcessExecutor().command(
                        "cmd",
                        "/c",
                        "mklink",
                        link.toAbsolutePath().toString(),
                        targetWin.toAbsolutePath().toString())
                        .timeout(2500L, TimeUnit.MILLISECONDS)
                        .start();

                int err = cmd.waitForExit();
                if (err != 0) {
                    throw new RuntimeException("Command execution failed: " + err);
                }

            } catch (InterruptedException | RuntimeException e) {

                // Fallback to use file copying...
                m_logger.warn("No mklink found or call failed. Please verify "
                        + "that the spikex user has \"Create symbolic links\" "
                        + "set in the local policy.", e);

                m_logger.info("Copying \"{}\" to \"{}\"", targetWin, link);

                java.nio.file.Files.copy(
                        targetWin.toAbsolutePath(),
                        link.toAbsolutePath(),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            throw new IOException("Unsupported operating system: "
                    + HostOs.operatingSystem());
        }

        return link.toAbsolutePath();
    }

    public static void deleteDirectory(
            final Path dir,
            final boolean ignoreIOException) throws IOException {

        java.nio.file.Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                try {
                    java.nio.file.Files.delete(file);
                } catch (IOException e) {
                    if (!ignoreIOException) {
                        throw e;
                    } else {
                        m_logger.warn("Failed to delete file: {} reason: {}",
                                file, e.toString());
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e1) throws IOException {
                if (e1 == null) {
                    try {
                        java.nio.file.Files.delete(dir);
                    } catch (IOException e2) {
                        if (!ignoreIOException) {
                            throw e2;
                        } else {
                            m_logger.warn("Failed to delete directory: {} reason: {}",
                                    dir, e2.toString());
                        }
                    }
                    return FileVisitResult.CONTINUE;
                } else {
                    throw e1;
                }
            }
        });
    }

    public static String resolveFilename(final Path file) {
        // Sanity check
        Preconditions.checkNotNull(file);
        String name = file.getFileName().toString();
        int pos = name.lastIndexOf('.');
        if (pos > 0) {
            name = name.substring(0, pos);
        }
        return name;
    }

    public static String resolveSuffix(final Path file) {
        // Sanity check
        Preconditions.checkNotNull(file);
        String suffix = "";
        String name = file.getFileName().toString();
        int pos = name.lastIndexOf('.');
        if (pos > 0) {
            suffix = name.substring(pos);
        }
        return suffix;
    }

    public static int hashOfFile(final Path file) throws IOException {
        return XXHash32.hashOfFile(file, HASH_SALT);
    }
}
