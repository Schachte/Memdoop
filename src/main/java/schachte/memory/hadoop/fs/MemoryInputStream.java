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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSInputStream;


/**
 * An input stream which wraps {@link ByteArrayInputStream} and implements the
 * Hadoop file system input stream interface.
 * 
 * This class is package private since it's only purpose is to support
 * {@link InMemoryFileSystem}.
 */
class MemoryInputStream extends FSInputStream {
	private final MyByteArrayInputStream bais;
	private final FileNode fileNode;

	MemoryInputStream(FileNode fileNode) {
		this.bais = new MyByteArrayInputStream(fileNode.bytes);
		this.fileNode = fileNode;
	}

	@Override
	public void seek(long pos) throws IOException {
		this.bais.reset();
		this.bais.skip(pos);
	}

	@Override
	public long getPos() throws IOException {
		return this.bais.getPos();
	}

	/**
	 * This method does nothing since there are no alternate data sources
	 * supported by the file system.
	 * 
	 * @return <code>false</code>
	 */
	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}

	@Override
	public int read() throws IOException {
		if (!this.fileNode.isOpen()) {
			throw new IOException("File is closed: " + this.fileNode.getPath());
		}
		return this.bais.read();
	}

	@Override
	public void close() throws IOException {
		this.fileNode.setOpen(false);
	}

	/**
	 * Extend ByteArrayInputStream to get access to protected state.
	 */
	private class MyByteArrayInputStream extends ByteArrayInputStream {

		public MyByteArrayInputStream(byte[] buf) {
			super(buf);
		}

		public int getPos() {
			return pos;
		}
	}
}