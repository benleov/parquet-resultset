package parquet.resultset.impl;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A parquet {@link OutputFile} that writes to an output stream, so parquet can be written without a file. Parquet
 * files are written sequentially, so the stream only needs to track its position.
 */
class StreamOutputFile implements OutputFile {

    private final OutputStream outputStream;

    StreamOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {

        return new PositionOutputStream() {

            private long position;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) throws IOException {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                position += len;
            }

            @Override
            public void flush() throws IOException {
                outputStream.flush();
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
            }
        };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
