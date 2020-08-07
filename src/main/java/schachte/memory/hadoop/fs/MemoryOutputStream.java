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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps a ByteArrayOutputStream as an OutputStream for the purpose of being
 * wrapped in a FSDataOutputStream
 */
class MemoryOutputStream extends OutputStream {
	private ByteArrayOutputStream baos = new ByteArrayOutputStream();
	private final FileNode fileNode;

	MemoryOutputStream(FileNode fileNode) {
		this.fileNode = fileNode;
		try {
			baos.write(fileNode.bytes);
		} catch (IOException e) {
			// This should never happen
			throw new RuntimeException("Unexpected error: "
					+ e.getLocalizedMessage(), e);
		}
	}

	@Override
	public void write(int b) throws IOException {
		if (baos == null) {
			throw new IOException("File closed!");
		}
		this.baos.write(b);
	}

	@Override
	public void close() throws IOException {
		if (baos != null) {
			this.fileNode.bytes = baos.toByteArray();
			this.fileNode.setOpen(false);
			baos = null;
		}
	}
}