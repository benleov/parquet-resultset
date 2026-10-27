package parquet.resultset.impl;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps the caller's output stream so that writers which close their stream when finished only flush it.
 */
class NonClosingOutputStream extends FilterOutputStream {

    NonClosingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // FilterOutputStream writes arrays one byte at a time
        out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
