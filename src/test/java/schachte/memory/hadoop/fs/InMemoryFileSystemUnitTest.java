/**
 * Copyright 2013 Red Arch Solutions, Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package schachte.memory.hadoop.fs;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class InMemoryFileSystemUnitTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	Path path = new Path("/message.txt");
	final String message = "Hello World!";

	private final Configuration configuration = new Configuration();
	
	InMemoryFileSystem inMemoryFileSystem;
	FsPermission allReadOnly = new FsPermission((short) 0444);

	@Before
	public void setUp() {
		inMemoryFileSystem = InMemoryFileSystem.get(configuration);
	}

	@After
	public void tearDown() {
		InMemoryFileSystem.resetFileSystemState(configuration);
	}

	//
	// Test InMemoryFileSystem.get(Configuration)
	//

	@Test
	public void testGetNullConfiguration() {
		expectNullArgumentException("conf");
		InMemoryFileSystem.get(null);
	}

	@Test
	public void testGetConfiguration() throws IOException {
		Configuration conf = new Configuration();
		InMemoryFileSystem.get(conf);
		assertThat("Wrong default file system name",
				conf.get(InMemoryFileSystem.FS_DEFAULT_NAME_KEY),
				is(equalTo(InMemoryFileSystem.NAME.toString())));
		assertThat("Wrong implementation class",
				conf.get(InMemoryFileSystem.CONFIG_IMPL_CLASS_KEY),
				is(equalTo(InMemoryFileSystem.class.getName())));
		FileSystem fs = FileSystem.get(conf);
		assertTrue("Wrong default file system returned: "
				+ fs.getClass().getName(), fs instanceof InMemoryFileSystem);
	}

	@Test
	public void testGetUri() {
		assertThat("Wrong URI", inMemoryFileSystem.getUri(),
				is(equalTo(InMemoryFileSystem.NAME)));
	}

	//
	// Test setWorkingDirectory(Path)/getWorkingDirectory()
	//

	@Test
	public void testSetWorkingDirectoryNull() {
		expectNullArgumentException("path");
		inMemoryFileSystem.setWorkingDirectory(null);
	}

	@Test
	public void testSetWorkingDirectoryRelative() throws IOException {
		Path path = new Path("mydir");
		inMemoryFileSystem.mkdirs(path);

		inMemoryFileSystem.setWorkingDirectory(path);
		assertThat("Wrong working directory",
				inMemoryFileSystem.getWorkingDirectory(), is(equalTo(new Path(
						new Path("/"), path))));
	}

	@Test
	public void testSetWorkingDirectoryNonExistentDirectory() {
		Path path = new Path("mydir");
		expectIllegalArgumentException("'/" + path + "' not found!");
		inMemoryFileSystem.setWorkingDirectory(path);
	}

	@Test
	public void testSetWorkingDirectoryToFile() throws IOException {
		Path path = new Path("message.txt");
		inMemoryFileSystem.create(path);

		expectIllegalArgumentException("'/" + path + "' is not a directory!");
		inMemoryFileSystem.setWorkingDirectory(path);
	}

	@Test
	public void testGetWorkingDirectoryInitial() {
		Path workingDir = inMemoryFileSystem.getWorkingDirectory();
		assertThat("Wrong initial working directory", workingDir,
				is(equalTo(new Path("/"))));
	}

	//
	// Test create(path)/open(path)/close()
	//

	@Test
	public void testCreateNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.create(null, null, false, 0, (short) 0, 0, null);
	}

	@Test
	public void testCreateOpenLoop() throws IOException {
		writeMessage(path);
		FileStatus fileStatus = inMemoryFileSystem.getFileStatus(path);
		assertThat("Wrong file length", (int) fileStatus.getLen(),
				is(equalTo(message.getBytes().length)));

		assertThat("Wrong message", readMessage(path), is(equalTo(message)));
	}

	/**
	 * This test verifies that we can create a file with one file system
	 * instance and then read it with another. In other words, that two
	 * different instances share the same file system state.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testCreateOpenLoopTwoFileSystemInstances() throws IOException {
		writeMessage(path);
		// Change the file system instance...
		inMemoryFileSystem = InMemoryFileSystem.get(inMemoryFileSystem.getConf());
		assertThat("Wrong message", readMessage(inMemoryFileSystem, path), is(equalTo(message)));
	}

	@Test
	public void testWriteToClosedStream() throws IOException {
		FSDataOutputStream out = inMemoryFileSystem.create(path);
		out.close();

		expectIOException("File closed!");
		out.writeBytes("Don't do it!");
	}

	@Test
	public void testOpenOnOpenFileFromCreate() throws IOException {
		inMemoryFileSystem.create(path);

		expectIOException("File already open: " + path);
		inMemoryFileSystem.open(path);
	}

	@Test
	public void testOpenOnOpenFileFromOpen() throws IOException {
		writeMessage(path);
		inMemoryFileSystem.open(path);

		expectIOException("File already open: " + path);
		inMemoryFileSystem.open(path);
	}

	//
	// InputStream Tests
	//

	@Test
	public void testInputStreamClose() throws IOException {
		writeMessage(path);
		FSDataInputStream in = inMemoryFileSystem.open(path);
		in.close();

		expectIOException("File is closed: " + path);
		in.read();
	}

	@Test
	public void testInputStreamSeek() throws IOException {
		Path path = new Path("message.txt");
		writeMessage(path);
		byte[] bytesOut = message.getBytes();

		long seekPosition = 6;
		FSDataInputStream in = inMemoryFileSystem.open(path);
		byte[] bytesIn = new byte[bytesOut.length - (int) seekPosition];
		in.seek(seekPosition);
		assertThat("Wrong position after seek", in.getPos(),
				is(equalTo(seekPosition)));
		in.read(bytesIn);
		for (int i = 6; i < bytesOut.length; i++) {
			assertThat("Wrong byte at index " + i, bytesIn[i - 6],
					is(equalTo(bytesOut[i])));
		}
	}

	//
	// create() Tests
	//

	@Test
	public void testCreateExistsNoOverwrite() throws IOException {
		inMemoryFileSystem.create(path);

		expectIOException("File already exists: " + path);
		inMemoryFileSystem.create(path, false);
	}

	@Test
	public void testCreateExistsOverwrite() throws IOException {
		writeMessage(path);

		String secondMsg = "Goodbye World!";
		FSDataOutputStream out = inMemoryFileSystem.create(path, true);
		out.writeBytes(secondMsg);
		out.close();

		assertThat("Wrong message in file", readMessage(path),
				is(equalTo(secondMsg)));
	}

	@Test
	public void TestCreateFileWithExistingDirnameOverwrite() throws IOException {
		Path dir = new Path("/message");
		inMemoryFileSystem.mkdirs(dir);

		expectIOException("Can't overwrite a directory with a file: " + dir);
		inMemoryFileSystem.create(dir, true);
	}

	@Test
	public void testCreateFilePathWrongScheme() throws IOException {
		Path path = new Path("file:///");
		expectIllegalArgumentException("Wrong file system: "
				+ path.toUri().getScheme() + ", expected: "
				+ inMemoryFileSystem.getUri().getScheme());
		inMemoryFileSystem.create(path);
	}

	@Test
	public void testCreateRelativeFile() throws IOException {
		Path dirPath = new Path("/mydir");
		inMemoryFileSystem.mkdirs(dirPath);
		inMemoryFileSystem.setWorkingDirectory(dirPath);

		Path relativeFile = new Path("message.txt");
		writeMessage(relativeFile);

		Path absoluteFile = new Path(dirPath + "/" + relativeFile);

		assertThat("Wrong message", readMessage(absoluteFile),
				is(equalTo(message)));
		FileStatus fstatus = inMemoryFileSystem.getFileStatus(absoluteFile);
		assertFalse("isDirectory() returned true on file", fstatus.isDir());
		assertThat("getLen() returned wrong value", fstatus.getLen(),
				is(equalTo((long) message.getBytes().length)));
	}

	@Test
	public void testStaticCreateFileNullFileSystem() throws IOException{
		expectIllegalArgumentException("fs == null not allowed!");
		InMemoryFileSystem.createFile(null, path, "Hello World!");
	}

	@Test
	public void testStaticCreateFileNullPath() throws IOException{
		expectIllegalArgumentException("path == null not allowed!");
		InMemoryFileSystem.createFile(inMemoryFileSystem, null, "Hello World!");
	}
	
	@Test
	public void testStaticCreateFileNullContent() throws IOException{
		expectIllegalArgumentException("contents == null not allowed!");
		InMemoryFileSystem.createFile(inMemoryFileSystem, path, null);
	}
	
	//
	// open() Tests
	//

	@Test
	public void testOpenNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.open(null);
	}

	@Test
	public void testOpenDirectory() throws IOException {
		Path dirPath = new Path("mydir");
		inMemoryFileSystem.mkdirs(dirPath);

		try {
			inMemoryFileSystem.open(dirPath, 4096);
		} catch (IOException e) {
			assertThat("Wrong exception message", e.getLocalizedMessage(),
					is(equalTo("'/" + dirPath + "' is not a file!")));
		}
	}

	@Test
	public void testOpenNoSuchFile() throws IOException {
		Path path = new Path("/nosuchfile.txt");
		expectPathMustExistIOException(path);
		inMemoryFileSystem.open(path);
	}

	//
	// getFileStatus() Tests
	//

	@Test
	public void testGetFileStatusNoSuchPath() throws IOException {
		Path path = new Path("/mydoc.txt");
		expectPathMustExistIOException(path);
		inMemoryFileSystem.getFileStatus(path);
	}

	//
	// mkdirs() Tests
	//

	@Test
	public void testMkdirsNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.mkdirs(null, allReadOnly);
	}

	@Test
	public void testMkdirs() throws IOException {
		Path path = new Path("java");

		inMemoryFileSystem.mkdirs(path, allReadOnly);
		FileStatus fStatus = inMemoryFileSystem.getFileStatus(path);
		assertNotNull("getFileStatus() returned null", fStatus);
		assertTrue("Not a directory!", fStatus.isDir());
		assertThat("Wrong length for directory", fStatus.getLen(),
				is(equalTo(0l)));
		assertThat("Wrong permissions", fStatus.getPermission(),
				is(equalTo(allReadOnly)));
	}

	@Test
	public void testMkdirsNullPermissions() throws IOException {
		Path path = new Path("java");

		inMemoryFileSystem.mkdirs(path, null);
		FileStatus fStatus = inMemoryFileSystem.getFileStatus(path);
		assertThat("Wrong permissions", fStatus.getPermission(),
				is(equalTo(new FsPermission(
						InMemoryFileSystem.DEFAULT_PERMISSION))));
	}

	@Test
	public void testMkdirsTwice() throws IOException {
		Path path = new Path("java");
		inMemoryFileSystem.mkdirs(path);
		inMemoryFileSystem.mkdirs(path);
	}

	@Test
	public void testMkdirsTwoLevels() throws IOException{
		Path parentDir = new Path("/src/main/java");
		inMemoryFileSystem.mkdirs(parentDir);
		
		Path childDir = new Path(parentDir, "ras/");
		inMemoryFileSystem.mkdirs(childDir);
		
		FileStatus[] fstats = inMemoryFileSystem.listStatus(parentDir);
		assertThat("Wrong number of wrong number of childern",fstats.length, is(equalTo(1)));
		assertThat("Wrong child",fstats[0].getPath(), is(equalTo(childDir)));
	}
	
	//
	// Delete Tests
	//

	@Test
	public void testDeleteNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.delete(null, true);
	}

	@Test
	public void testDeleteNonExistentPath() throws IOException {
		Path path = new Path("/nosuchfileordirectory");
		expectPathMustExistIOException(path);
		inMemoryFileSystem.delete(path, true);
	}

	@Test
	public void testDeleteDirectory() throws IOException {
		Path path = new Path("java");
		inMemoryFileSystem.mkdirs(path);

		assertTrue("delete() returned false",
				inMemoryFileSystem.delete(path, false));

		expectPathMustExistIOException(path);
		inMemoryFileSystem.getFileStatus(path);
	}

	@Test
	public void testDeleteFile() throws IOException {
		Path path = new Path("data.txt");
		InMemoryFileSystem.createFile(inMemoryFileSystem, path, "Hello World");
		assertTrue("Test setup failed!", inMemoryFileSystem.exists(path));
		inMemoryFileSystem.delete(path, false);
		assertFalse("Delete failed!", inMemoryFileSystem.exists(path));
	}

	@Test
	public void testDeleteDirectoryContainingDirectoryRecursive()
			throws IOException {
		Path baseDir = new Path("/src");
		Path subDir = new Path(baseDir + "/java");
		inMemoryFileSystem.mkdirs(subDir);

		assertTrue("delete() returned false",
				inMemoryFileSystem.delete(baseDir, true));

		// See if the sub-directory still exists...
		expectPathMustExistIOException(subDir);
		inMemoryFileSystem.getFileStatus(subDir);
	}

	@Test
	public void testDeleteDirectoryContainingDirectoryNonRecursive()
			throws IOException {
		Path baseDir = new Path("/src");
		Path subDir = new Path(baseDir + "/java");
		inMemoryFileSystem.mkdirs(subDir);

		// Attempt to delete directory, non-recursive, with sub-directory
		try {
			inMemoryFileSystem.delete(baseDir, false);
			fail("delete() of non-empty directory succeeded!");
		} catch (IOException expected) {
			assertThat("Wrong exception message",
					expected.getLocalizedMessage(), is(equalTo("Directory '"
							+ baseDir + "' is not empty!")));
		}

		assertNotNull("FileStatus == null after failed delete attempt",
				inMemoryFileSystem.getFileStatus(baseDir));
	}

	@Test
	public void testDeleteDirectoryContainingFilesRecursive()
			throws IOException {
		Path dirPath = new Path("/doc");
		Path filePath = new Path(dirPath + "/message.txt");
		writeMessage(filePath);

		inMemoryFileSystem.delete(dirPath, true);

		expectPathMustExistIOException(filePath);
		inMemoryFileSystem.getFileStatus(filePath);
	}

	@Test
	public void testDeleteOpenFile() throws IOException {
		Path file = new Path("/myfile.txt");
		inMemoryFileSystem.create(file);

		expectIOException("Delete failed, resource is in use: " + file);
		inMemoryFileSystem.delete(file, false);
	}

	@Test
	public void testDeleteDirectoryWithOpenFile() throws IOException {
		Path dir = new Path("/mydir");
		Path file = new Path(dir, "myfile.txt");
		inMemoryFileSystem.create(file);

		expectIOException("Delete failed, resource is in use: " + file);
		inMemoryFileSystem.delete(dir, true);
	}

	//
	// Append tests
	//

	@Test
	public void testAppendNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.append(null, 0, null);
	}

	@Test
	public void testAppendNonExistentFile() throws IOException {
		Path path = new Path("nosuchfile");
		expectPathMustExistIOException(path);
		inMemoryFileSystem.append(path);
	}

	@Test
	public void testAppendDirectory() throws IOException {
		Path path = new Path("/mydir");
		inMemoryFileSystem.mkdirs(path);

		expectIOException("'" + path + "' is not a file!");
		inMemoryFileSystem.append(path);
	}

	@Test
	public void testAppend() throws IOException {
		writeMessage(path);

		String msgPart2 = " Goodbye World!";
		FSDataOutputStream out = inMemoryFileSystem.append(path);
		out.writeBytes(msgPart2);
		out.close();
		String twoMessage = message + msgPart2;
		assertThat("Wrong message", readMessage(path), is(equalTo(twoMessage)));
	}

	@Test
	public void testAppendOnOpenFileFromCreate() throws IOException {
		inMemoryFileSystem.create(path);

		expectIOException("File already open: " + path);
		inMemoryFileSystem.append(path);
	}

	//
	// List Status Tests
	//

	@Test
	public void testListStatusNullPath() throws IOException {
		expectNullArgumentException("path");
		inMemoryFileSystem.listStatus((Path) null);
	}

	@Test
	public void testListStatusFile() throws IOException {
		Path path = new Path("message.txt");
		inMemoryFileSystem.create(path);

		FileStatus[] fileStatusList = inMemoryFileSystem.listStatus(path);
		assertThat("Wrong file status list", fileStatusList,
				is(equalTo(new FileStatus[] { inMemoryFileSystem
						.getFileStatus(path) })));
	}

	@Test
	public void testListStatusEmptyDir() throws IOException {
		Path path = new Path("/mydir");
		inMemoryFileSystem.mkdirs(path);

		FileStatus[] fileStatusList = inMemoryFileSystem.listStatus(path);
		assertThat("Wrong file status list", fileStatusList,
				is(equalTo(new FileStatus[0])));
	}

	@Test
	public void testListStatusDirWithSubDirAndFile() throws IOException {
		Path dir = new Path("/mydir");
		Path subDir = new Path(dir + "/subdir");
		Path file = new Path(dir + "/message.txt");

		inMemoryFileSystem.mkdirs(subDir);
		inMemoryFileSystem.create(file);

		Set<FileStatus> expectedStatus = new HashSet<FileStatus>();
		expectedStatus.add(inMemoryFileSystem.getFileStatus(subDir));
		expectedStatus.add(inMemoryFileSystem.getFileStatus(file));

		Set<FileStatus> resultStatus = new HashSet<FileStatus>(
				Arrays.asList(inMemoryFileSystem.listStatus(dir)));
		assertThat("Wrong file status list", resultStatus,
				is(equalTo(expectedStatus)));
	}

	//
	// SetOwner Tests
	//

	@Test
	public void testSetOwnerAndGroupNonExistentPath() throws IOException {
		Path path = new Path("/nosuchdir/nosuchfile.txt");
		expectPathMustExistIOException(path);
		inMemoryFileSystem.setOwner(path, "me", "mygroup");
	}

	@Test
	public void testSetOwnerAndGroup() throws IOException {
		String owner = "me";
		String group = "mygroup";
		Path dir = new Path("/mydir");
		inMemoryFileSystem.mkdirs(dir);
		inMemoryFileSystem.setOwner(dir, owner, group);
		FileStatus fs = inMemoryFileSystem.getFileStatus(dir);
		assertThat("Wrong owner for directory", fs.getOwner(),
				is(equalTo(owner)));
		assertThat("Wrong group for directory", fs.getGroup(),
				is(equalTo(group)));

		// Test relative file path...
		inMemoryFileSystem.setWorkingDirectory(dir);
		Path file = new Path("myfile.txt");
		inMemoryFileSystem.create(file);
		inMemoryFileSystem.setOwner(file, owner, group);
		fs = inMemoryFileSystem.getFileStatus(file);
		assertThat("Wrong owner for file", fs.getOwner(), is(equalTo(owner)));
		assertThat("Wrong group for file", fs.getGroup(), is(equalTo(group)));
	}

	//
	// SetUser Tests
	//

	@Test
	public void setUserNull() {
		expectNullArgumentException("user");
		inMemoryFileSystem.setUser(null);
	}

	@Test
	public void setUserGroupsNull() {
		expectNullArgumentException("groups");
		inMemoryFileSystem.setUser("me", (String[]) null);
	}

	@Test
	public void setUserGroupsContainsNull() {
		expectNullArgumentException("groups[i]");
		inMemoryFileSystem.setUser("me", (String) null);
	}

	@Test
	public void setUserGroupsContainsEmptyString() {
		expectIllegalArgumentException("groups[i].length() == 0 not allowed!");
		inMemoryFileSystem.setUser("me", "");
	}

	@Test
	public void testSetUserNoGroups() throws IOException {
		String user = "george";
		inMemoryFileSystem.setUser(user);

		assertThat("Wrong user", inMemoryFileSystem.getUser(),
				is(equalTo(user)));
		assertThat("Wrong userGroups", inMemoryFileSystem.getUserGroups(),
				is(equalTo(InMemoryFileSystem.DEFAULT_USERS_GROUPS)));

		Path dir = new Path("/mydir");
		inMemoryFileSystem.mkdirs(dir);
		FileStatus fs = inMemoryFileSystem.getFileStatus(dir);
		assertThat("Wrong owner for directory", fs.getOwner(),
				is(equalTo(user)));
		Path file = new Path("myfile.txt");
		inMemoryFileSystem.create(file);
		fs = inMemoryFileSystem.getFileStatus(file);
		assertThat("Wrong owner for file", fs.getOwner(), is(equalTo(user)));
		assertThat("Wrong group for directory", fs.getGroup(),
				is(equalTo(InMemoryFileSystem.DEFAULT_GROUP)));
	}

	@Test
	public void testSetUserAndGroups() throws IOException {
		String user = "bob";
		String group = "myteam";
		inMemoryFileSystem.setUser(user, group);

		assertThat("Wrong user", inMemoryFileSystem.getUser(),
				is(equalTo(user)));
		assertThat(
				"Wrong userGroups",
				inMemoryFileSystem.getUserGroups(),
				is(equalTo((Set<String>) new HashSet<String>(Arrays
						.asList(group)))));
		FsPermission permission = new FsPermission((short) 0700);
		Path dir = new Path("/mydir");
		inMemoryFileSystem.mkdirs(dir, permission);
		inMemoryFileSystem.setOwner(dir, user, group);
		FileStatus fs = inMemoryFileSystem.getFileStatus(dir);
		assertThat("Wrong owner for directory", fs.getOwner(),
				is(equalTo(user)));
		assertThat("Wrong group for directory", fs.getGroup(),
				is(equalTo(group)));

		Path file = new Path("myfile.txt");
		inMemoryFileSystem.create(file);
		inMemoryFileSystem.setOwner(file, user, group);
		fs = inMemoryFileSystem.getFileStatus(file);
		assertThat("Wrong owner for file", fs.getOwner(), is(equalTo(user)));
		assertThat("Wrong group for directory", fs.getGroup(),
				is(equalTo(group)));
	}

	//
	// Permission Tests
	//

	@Test
	public void testMkdirsOwnerNoWrite() throws IOException {
		Path parentDir = new Path("/parentDir");
		inMemoryFileSystem.mkdirs(parentDir, allReadOnly);
		inMemoryFileSystem.setWorkingDirectory(parentDir);
		Path subDir = new Path("subDir");
		expectIOException("Permission denied!");
		inMemoryFileSystem.mkdirs(subDir);
	}

	@Test
	public void testMkdirsNotOwnerInGroupWithWrite() throws IOException {
		Path parentDir = new Path("/parentDir");
		inMemoryFileSystem.mkdirs(parentDir, new FsPermission((short) 0660));
		inMemoryFileSystem.setUser("bill"); // default user groups
		inMemoryFileSystem.setWorkingDirectory(parentDir);

		Path subDir = new Path("subDir");
		inMemoryFileSystem.mkdirs(subDir);
		FileStatus fStatus = inMemoryFileSystem.getFileStatus(subDir);
		assertThat("Wrong owner", fStatus.getOwner(), is(equalTo("bill")));
	}

	@Test
	public void testMkdirsNotOwnerNotInGroupNoWorldWrite() throws IOException {
		Path parentDir = new Path("/parentDir");
		inMemoryFileSystem.mkdirs(parentDir, new FsPermission((short) 0664));
		inMemoryFileSystem.setUser("bill", "underprivileged");
		inMemoryFileSystem.setWorkingDirectory(parentDir);

		Path subDir = new Path("subDir");
		expectIOException("Permission denied!");
		inMemoryFileSystem.mkdirs(subDir);
	}

	@Test
	public void testMkdirsNoWorldWriteNoGroupWriteOwnerHasWriteNotOwner()
			throws IOException {
		Path parentDir = new Path("/parentDir");
		inMemoryFileSystem.mkdirs(parentDir, new FsPermission((short) 0600));
		inMemoryFileSystem.setUser("bill", "underprivileged");
		inMemoryFileSystem.setWorkingDirectory(parentDir);

		Path subDir = new Path("subDir");
		expectIOException("Permission denied!");
		inMemoryFileSystem.mkdirs(subDir);
	}

	/*
	 * We only need this one permission test for create because the permission
	 * code is reused between mkdirs() and create(). The mkdirs() unit tests
	 * cover the permission logic. This test simply ensures that create() uses
	 * the permissions logic.
	 */
	@Test
	public void testCreateParentDirPathOwnerNoWrite() throws IOException {
		Path parentDir = new Path("/parentDir");
		inMemoryFileSystem.mkdirs(parentDir, allReadOnly);
		inMemoryFileSystem.setWorkingDirectory(parentDir);

		Path file = new Path("myfile.txt");
		expectIOException("Permission denied!");
		inMemoryFileSystem.create(file);
	}

	/*
	 * We only need this one permission test for open() because the permission
	 * code is reused the mkdirs() unit tests cover the permission logic. This
	 * test simply ensures that open() uses the permissions logic with the
	 * correct FsAction.
	 */
	@Test
	public void testOpenOwnerNoReadPermission() throws IOException {
		Path file = new Path("myfile.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(file,
				new FsPermission((short) 0333), true, 0, (short) 0, 0, null);
		out.close();

		expectIOException("Permission denied!");
		inMemoryFileSystem.open(file);
	}

	/*
	 * We only need this one permission test for open() because the permission
	 * code is reused and the mkdirs() unit tests cover the permission logic.
	 * This test simply ensures that open() uses the permissions logic with the
	 * correct FsAction.
	 */
	@Test
	public void testListStatusOwnerNoReadPermission() throws IOException {
		Path dir = new Path("noreadDir");
		inMemoryFileSystem.mkdirs(dir, new FsPermission((short) 0333));

		expectIOException("Permission denied!");
		inMemoryFileSystem.listStatus(dir);
	}

	/*
	 * We only need this one permission test for append() because the permission
	 * code is reused and the mkdirs() unit tests cover the permission logic.
	 * This test simply ensures that append() uses the permissions logic with
	 * the correct FsAction.
	 */
	@Test
	public void testAppendOwnerNoWrite() throws IOException {
		Path file = new Path("myfile.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(file, allReadOnly,
				true, 0, (short) 0, 0, null);
		out.close();

		expectIOException("Permission denied!");
		inMemoryFileSystem.append(file);
	}

	/*
	 * We only need this one permission test for delete() because the permission
	 * code is reused and the mkdirs() unit tests cover the permission logic.
	 * This test simply ensures that delete() uses the permissions logic with
	 * the correct FsAction.
	 */
	@Test
	public void testDeleteOwnerNoWrite() throws IOException {
		Path file = new Path("myfile.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(file, allReadOnly,
				true, 0, (short) 0, 0, null);
		out.close();

		expectIOException("Permission denied!");
		inMemoryFileSystem.delete(file, true);
	}

	//
	// rename() Tests
	//

	@Test
	public void testRenameNullSource() throws IOException {
		expectNullArgumentException("src");
		inMemoryFileSystem.rename(null, new Path("destination"));
	}

	@Test
	public void testRenameNullDestination() throws IOException {
		expectNullArgumentException("dst");
		inMemoryFileSystem.rename(new Path("source"), null);
	}

	@Test
	public void testRenameSourceDoesNotExist() throws IOException {
		Path source = new Path("source");
		expectPathMustExistIOException(source);
		inMemoryFileSystem.rename(source, new Path("destination"));
	}

	@Test
	public void testRenameDestinationExist() throws IOException {
		Path source = new Path("/source");
		Path destination = new Path("/destination");
		inMemoryFileSystem.mkdirs(source);
		inMemoryFileSystem.mkdirs(destination);
		expectIOException("Rename failed, destination already exists: "
				+ destination);
		inMemoryFileSystem.rename(source, destination);
	}

	@Test
	public void testRenameEmptyDir() throws IOException {
		Path source = new Path("/source");
		Path destination = new Path("/destination");
		inMemoryFileSystem.mkdirs(source);
		inMemoryFileSystem.rename(source, destination);

		FileStatus fstatus = inMemoryFileSystem.getFileStatus(destination);
		assertTrue("Destination not a directory", fstatus.isDir());
		assertFalse("Original path still exists!",
				inMemoryFileSystem.exists(source));
		expectPathMustExistIOException(source);
		inMemoryFileSystem.getFileStatus(source);
	}

	@Test
	public void testRenameFile() throws IOException {
		Path source = new Path("/source.txt");
		Path destination = new Path("/destination.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(source);
		String message = "Hello World!";
		out.writeBytes(message);
		out.close();

		inMemoryFileSystem.rename(source, destination);
		FSDataInputStream in = inMemoryFileSystem.open(destination);
		byte[] bytes = new byte[message.getBytes().length];
		in.read(bytes);
		assertThat("Wrong message", new String(bytes), is(equalTo(message)));

		expectIOException("'" + source + "' not found!");
		inMemoryFileSystem.getFileStatus(source);
	}

	@Test
	public void testRenameFileToNewDirectory() throws IOException {
		Path srcPath = new Path("/source.txt");
		Path dstDir = new Path("mydir");
		Path dstPath = new Path(dstDir, "destination.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(srcPath);
		out.close();

		inMemoryFileSystem.rename(srcPath, dstPath);
		FileStatus fstatus = inMemoryFileSystem.getFileStatus(dstDir);
		assertTrue(fstatus.isDir());
		fstatus = inMemoryFileSystem.getFileStatus(dstPath);
		assertFalse(fstatus.isDir());
	}

	@Test
	public void testRenameNoWriteSource() throws IOException {
		Path source = new Path("/source.txt");
		Path destination = new Path("/destination.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(source, allReadOnly,
				false, 0, (short) 0, 0, null);
		out.close();

		expectIOException("Permission denied!");
		inMemoryFileSystem.rename(source, destination);
	}

	@Test
	public void testRenameNoWriteDestinationDir() throws IOException {
		Path source = new Path("/source.txt");
		Path destinationDir = new Path("mydir");
		inMemoryFileSystem.mkdirs(destinationDir, allReadOnly);
		Path destination = new Path(destinationDir, "destination.txt");
		FSDataOutputStream out = inMemoryFileSystem.create(source);
		out.close();

		expectIOException("Permission denied!");
		inMemoryFileSystem.rename(source, destination);
	}

	@Test
	public void testFileSystemCopyEmptyDirectory() throws IOException {
		InMemoryFileSystem srcFs = inMemoryFileSystem;
		InMemoryFileSystem dstFs = new InMemoryFileSystem("file");
		dstFs.setConf(srcFs.getConf());
		Path srcPath = new Path("emptyDir");
		srcFs.mkdirs(srcPath);

		Path dstParentPath = new Path("/");
		InMemoryFileSystem.copy(srcFs, srcPath, dstFs, dstParentPath);

		Path dstPath = new Path(dstParentPath, srcPath.getName());
		FileStatus fstatus = dstFs.getFileStatus(dstPath);
		assertNotNull("Destination directory not created!", fstatus);
	}

	@Test
	public void testFileSystemCopyDirectoryWithSubDir() throws IOException {
		InMemoryFileSystem srcFs = inMemoryFileSystem;
		InMemoryFileSystem dstFs = new InMemoryFileSystem("file");
		dstFs.setConf(srcFs.getConf());

		srcFs.mkdirs(new Path("parentDir/subDir/subSubDir"));
		writeMessage(srcFs, new Path("parentDir/parentMsg.txt"), message);

		Path srcPath = new Path("parentDir");
		Path dstParentPath = new Path("/");
		InMemoryFileSystem.copy(srcFs, srcPath, dstFs, dstParentPath);

		FileStatus fstatus = dstFs.getFileStatus(new Path(dstParentPath,
				"parentDir/subDir/subSubDir"));
		assertNotNull("Destination directory not created!", fstatus);
		assertThat("Wrong message in parentDir",
				readMessage(dstFs, new Path("/parentDir/parentMsg.txt")),
				is(equalTo(message)));
	}

	@Test
	public void testResetFileSystemStateNullConfiguration(){
		expectIllegalArgumentException("conf == null not allowed!");
		InMemoryFileSystem.resetFileSystemState(null);
	}
	
	@Test
	public void testMultipleContext(){
		Configuration diffConf = new Configuration();
		InMemoryFileSystem.configure(diffConf);
	}
	
	private void expectPathMustExistIOException(Path path) {
		thrown.expect(IOException.class);
		String absoluteChar = (path.isAbsolute()) ? "" : "/";
		thrown.expectMessage(equalTo("'" + absoluteChar + path + "' not found!"));
	}

	private void expectNullArgumentException(String argName) {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(equalTo(argName + " == null not allowed!"));
	}

	private void expectIllegalArgumentException(String message) {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(equalTo(message));
	}

	private void expectIOException(String message) {
		thrown.expect(IOException.class);
		thrown.expectMessage(equalTo(message));
	}

	private void writeMessage(Path path) throws IOException {
		writeMessage(inMemoryFileSystem, path, message);
	}

	static void writeMessage(FileSystem fs, Path path, String message)
			throws IOException {
		FSDataOutputStream out = fs.create(path);
		out.writeBytes(message);
		out.close();
	}

	private String readMessage(Path path) throws IOException {
		return readMessage(inMemoryFileSystem, path);
	}

	static String readMessage(FileSystem fs, Path path) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		return reader.readLine();
	}
}
