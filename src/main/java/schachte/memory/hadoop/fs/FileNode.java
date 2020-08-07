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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;


class FileNode extends Node{
	private static final byte[] EMPTY_BYTES = new byte[0];

	byte[] bytes = EMPTY_BYTES;
	private boolean open = false;

	FileNode(Path path, FsPermission permissions) {
		super(path, permissions);
	}

	public InputStream open() throws IOException {
		setOpen(true);
		return new MemoryInputStream(this);
	}

	public OutputStream append() throws IOException {
		setOpen(true);
		return new MemoryOutputStream(this);
	}

	public boolean isOpen() {
		return open;
	}

	public void setOpen(boolean open) throws IOException{
		if(open && this.open){
			throw new IOException("File already open: "+this.getPath());
		}
		this.open = open;
	}
}
