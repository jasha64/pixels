package io.pixelsdb.pixels.worker.vhive;

import io.netty.buffer.ByteBuf;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.writer.ColumnWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.Constants.DEFAULT_HDFS_BLOCK_SIZE;
import static io.pixelsdb.pixels.core.TypeDescription.writeTypes;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;
import static java.util.Objects.requireNonNull;

/**
 * PixelsWriterStreamImpl is an implementation of {@link PixelsWriter} that writes
 * ColumnChunks to a stream, for operator pipelining over HTTP.
 */
@NotThreadSafe
public class PixelsWriterStreamImpl implements PixelsWriter {
    private static final Logger LOGGER = LogManager.getLogger(io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.class);

    static final ByteOrder WRITER_ENDIAN;

    static {
        boolean littleEndian = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("column.chunk.little.endian"));
        if (littleEndian) {
            WRITER_ENDIAN = ByteOrder.LITTLE_ENDIAN;
        } else {
            WRITER_ENDIAN = ByteOrder.BIG_ENDIAN;
        }
    }

    private final TypeDescription schema;
    private final int pixelStride;
    private final int rowGroupSize;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    private final EncodingLevel encodingLevel;
    private final boolean partitioned;
    private final Optional<List<Integer>> partKeyColumnIds;
    /**
     * The number of bytes that each column chunk is aligned to.
     */
    private final int chunkAlignment;
    /**
     * The byte buffer padded to each column chunk for alignment.
     */
    private final byte[] chunkPaddingBuffer;

    private final ColumnWriter[] columnWriters;
    private final StatsRecorder[] fileColStatRecorders;
    private long fileContentLength;
    private int fileRowNum;

    private long writtenBytes = 0L;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;
    /**
     * Whether any current hash value has been set.
     */
    private boolean hashValueIsSet = false;
    private int currHashValue = 0;

    private final List<PixelsProto.RowGroupInformation> rowGroupInfoList;    // row group information in footer
    private final List<PixelsProto.RowGroupStatistic> rowGroupStatisticList; // row group statistic in footer

    private final ByteBuf bufWriter;
    private final List<TypeDescription> children;

    private final ExecutorService columnWriterService = Executors.newCachedThreadPool();

    private PixelsWriterStreamImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            ByteBuf bufWriter,
            EncodingLevel encodingLevel,
            boolean partitioned,
            Optional<List<Integer>> partKeyColumnIds) {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(pixelStride > 0, "pixel stripe is not positive");
        this.pixelStride = pixelStride;
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        this.encodingLevel = encodingLevel;
        this.partitioned = partitioned;
        this.partKeyColumnIds = requireNonNull(partKeyColumnIds, "partKeyColumnIds is null");
        this.chunkAlignment = Integer.parseInt(ConfigFactory.Instance().getProperty("column.chunk.alignment"));
        checkArgument(this.chunkAlignment >= 0, "column.chunk.alignment must >= 0");
        this.chunkPaddingBuffer = new byte[this.chunkAlignment];
        children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        this.columnWriters = new ColumnWriter[children.size()];
        fileColStatRecorders = new StatsRecorder[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            columnWriters[i] = newColumnWriter(children.get(i), pixelStride, encodingLevel, WRITER_ENDIAN);
            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

        this.rowGroupInfoList = new LinkedList<>();
        this.rowGroupStatisticList = new LinkedList<>();

        this.bufWriter = bufWriter;
    }

    public static class Builder {
        private TypeDescription builderSchema = null;
        private int builderPixelStride = 0;
        private int builderRowGroupSize = 0;
        private PixelsProto.CompressionKind builderCompressionKind = PixelsProto.CompressionKind.NONE;
        private int builderCompressionBlockSize = 1;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private EncodingLevel builderEncodingLevel = EncodingLevel.EL0;
        private boolean builderPartitioned = false;
        private Optional<List<Integer>> builderPartKeyColumnIds = Optional.empty();
        private ByteBuf builderBufWriter = null;

        private Builder() {
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setSchema(TypeDescription schema) {
            this.builderSchema = requireNonNull(schema);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPixelStride(int stride) {
            this.builderPixelStride = stride;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setRowGroupSize(int rowGroupSize) {
            this.builderRowGroupSize = rowGroupSize;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setCompressionKind(PixelsProto.CompressionKind compressionKind) {
            this.builderCompressionKind = requireNonNull(compressionKind);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setCompressionBlockSize(int compressionBlockSize) {
            this.builderCompressionBlockSize = compressionBlockSize;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setTimeZone(TimeZone timeZone) {
            this.builderTimeZone = requireNonNull(timeZone);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setEncodingLevel(EncodingLevel encodingLevel) {
            this.builderEncodingLevel = encodingLevel;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPartitioned(boolean partitioned) {
            this.builderPartitioned = partitioned;
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setPartKeyColumnIds(List<Integer> partitionColumnIds) {
            this.builderPartKeyColumnIds = Optional.ofNullable(partitionColumnIds);
            return this;
        }

        public io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder setBufWriter(ByteBuf bufWriter) {
            this.builderBufWriter = requireNonNull(bufWriter);
            return this;
        }

        public PixelsWriter build() throws PixelsWriterException {
            requireNonNull(this.builderSchema, "schema is not set");
            checkArgument(!requireNonNull(builderSchema.getChildren(),
                    "schema's children is null").isEmpty(), "schema is empty");
            checkArgument(this.builderPixelStride > 0, "pixels stride size is not set");
            checkArgument(this.builderRowGroupSize > 0, "row group size is not set");
            checkArgument(this.builderPartitioned ==
                            (this.builderPartKeyColumnIds.isPresent() && !this.builderPartKeyColumnIds.get().isEmpty()),
                    "partition column ids are present while partitioned is false, or vice versa");



            return new io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    builderBufWriter,
                    builderEncodingLevel,
                    builderPartitioned,
                    builderPartKeyColumnIds);
        }
    }

    public static io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder newBuilder() {
        return new io.pixelsdb.pixels.worker.vhive.PixelsWriterStreamImpl.Builder();
    }

    public TypeDescription getSchema() {
        return schema;
    }

    @Override
    public int getNumRowGroup() {
        return this.rowGroupInfoList.size();
    }

    @Override
    public int getNumWriteRequests() {
        return 0;
//        if (physicalWriter == null) {
//            return 0;
//        }
//        return (int) Math.ceil(writtenBytes / (double) physicalWriter.getBufferSize());
    }

    @Override
    public long getCompletedBytes() {
        return writtenBytes;
    }

    public int getPixelStride() {
        return pixelStride;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public PixelsProto.CompressionKind getCompressionKind() {
        return compressionKind;
    }

    public int getCompressionBlockSize() {
        return compressionBlockSize;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public EncodingLevel getEncodingLevel() {
        return encodingLevel;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException {
        checkArgument(!partitioned, "this file is hash partitioned, " +
                "use addRowBatch(rowBatch, hashValue) instead");
        /**
         * Issue #170:
         * ColumnWriter.write() returns the total size of the current column chunk,
         * thus we should set curRowGroupDataLength = 0 here at the beginning.
         */
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
        // If the current row group size has exceeded the row group size, write current row group.
        if (curRowGroupDataLength >= rowGroupSize) {
            writeRowGroup();
            curRowGroupNumOfRows = 0L;
            return false;
        }
        return true;
    }

    @Override
    public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue) throws IOException {
        checkArgument(partitioned, "this file is not hash partitioned, " +
                "use addRowBatch(rowBatch) instead");
        if (hashValueIsSet) {
            // As the current hash value is set, at lease one row batch has been added.
            if (currHashValue != hashValue) {
                // Write the current partition (row group) and add the row batch to a new partition.
                writeRowGroup();
                curRowGroupNumOfRows = 0L;
            }
        }
        currHashValue = hashValue;
        hashValueIsSet = true;
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
    }

    private void writeColumnVectors(ColumnVector[] columnVectors, int rowBatchSize) {
        CompletableFuture<?>[] futures = new CompletableFuture[columnVectors.length];
        AtomicInteger dataLength = new AtomicInteger(0);
        for (int i = 0; i < columnVectors.length; ++i) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ColumnWriter writer = columnWriters[i];
            ColumnVector columnVector = columnVectors[i];
            columnWriterService.execute(() ->
            {
                try {
                    dataLength.addAndGet(writer.write(columnVector, rowBatchSize));
                    future.complete(null);
                } catch (IOException e) {
                    throw new CompletionException("failed to write column vector", e);
                }
            });
            futures[i] = future;
        }
        CompletableFuture.allOf(futures).join();
        curRowGroupDataLength += dataLength.get();
    }

    /**
     * Close PixelsWriterStreamImpl, indicating the end of file.
     */
    @Override
    public void close() {
        try {
            if (curRowGroupNumOfRows != 0) {
                writeRowGroup();
            }
            writeFileTail();  // We might have to do it in the beginning because we are using a HTTP stream.
//            physicalWriter.close();
            for (ColumnWriter cw : columnWriters) {
                cw.close();
            }
            columnWriterService.shutdown();
            columnWriterService.shutdownNow();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private void writeRowGroup() throws IOException {
        int rowGroupDataLength = 0;

        PixelsProto.RowGroupStatistic.Builder curRowGroupStatistic =
                PixelsProto.RowGroupStatistic.newBuilder();
        PixelsProto.RowGroupInformation.Builder curRowGroupInfo =
                PixelsProto.RowGroupInformation.newBuilder();
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex =
                PixelsProto.RowGroupIndex.newBuilder();
        PixelsProto.RowGroupEncoding.Builder curRowGroupEncoding =
                PixelsProto.RowGroupEncoding.newBuilder();

        // reset each column writer and get current row group content size in bytes
        for (ColumnWriter writer : columnWriters) {
            // flush writes the isNull bit map into the internal output stream.
            writer.flush();
            rowGroupDataLength += writer.getColumnChunkSize();
            if (chunkAlignment != 0 && rowGroupDataLength % chunkAlignment != 0) {
                /*
                 * Issue #519:
                 * This is necessary as the prepare() method of some storage (e.g., hdfs)
                 * has to determine whether to start a new block, if the current block
                 * is not large enough.
                 */
                rowGroupDataLength += chunkAlignment - rowGroupDataLength % chunkAlignment;
            }
        }

        // write and flush row group content
        try {
            curRowGroupOffset = bufWriter.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
            if (curRowGroupOffset != -1) {
                // Issue #519: make sure to start writing the column chunks in the row group from an aligned offset.
                int tryAlign = 0;
                while (chunkAlignment != 0 && curRowGroupOffset % chunkAlignment != 0 && tryAlign++ < 2) {
                    int alignBytes = (int) (chunkAlignment - curRowGroupOffset % chunkAlignment);
                    bufWriter.writeBytes(chunkPaddingBuffer, 0, alignBytes);  // physicalWriter.append(chunkPaddingBuffer, 0, alignBytes);
                    writtenBytes += alignBytes;
                    curRowGroupOffset = bufWriter.writerIndex();  // physicalWriter.prepare(rowGroupDataLength);
                }
                if (tryAlign > 2) {
                    LOGGER.warn("failed to align the start offset of the column chunks in the row group");
                    throw new IOException("failed to align the start offset of the column chunks in the row group");
                }

                for (ColumnWriter writer : columnWriters) {
                    byte[] rowGroupBuffer = writer.getColumnChunkContent();
                    // System.out.println("Column offset: " + bufWriter.writerIndex() + ", size: " + rowGroupBuffer.length);
                    bufWriter.writeBytes(
                            rowGroupBuffer,
                            0, rowGroupBuffer.length);  // physicalWriter.append(rowGroupBuffer, 0, rowGroupBuffer.length);
                    writtenBytes += rowGroupBuffer.length;
                    // add align bytes to make sure the column size is the multiple of fsBlockSize
                    if (chunkAlignment != 0 && rowGroupBuffer.length % chunkAlignment != 0) {
                        int alignBytes = chunkAlignment - rowGroupBuffer.length % chunkAlignment;
                        bufWriter.writeBytes(chunkPaddingBuffer, 0, alignBytes);  // physicalWriter.append(chunkPaddingBuffer, 0, alignBytes);
                        writtenBytes += alignBytes;
                    }
                }
                // physicalWriter.flush();
            } else {
                LOGGER.warn("write row group prepare failed");
                throw new IOException("write row group prepare failed");
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw e;
        }

        // update index and stats
        rowGroupDataLength = 0;
        for (int i = 0; i < columnWriters.length; i++) {
            ColumnWriter writer = columnWriters[i];
            PixelsProto.ColumnChunkIndex.Builder chunkIndexBuilder = writer.getColumnChunkIndex();
            chunkIndexBuilder.setChunkOffset(curRowGroupOffset + rowGroupDataLength);
            chunkIndexBuilder.setChunkLength(writer.getColumnChunkSize());
            rowGroupDataLength += writer.getColumnChunkSize();
            if (chunkAlignment != 0 && rowGroupDataLength % chunkAlignment != 0) {
                /*
                 * Issue #519:
                 * This line must be consistent with how column chunks are padded above.
                 * If we only pad after each column chunk when writing it into the physical writer
                 * without checking the alignment of the current position of the physical writer,
                 * then we should not consider curRowGroupOffset when calculating the alignment here.
                 */
                rowGroupDataLength += chunkAlignment - rowGroupDataLength % chunkAlignment;
            }
            // collect columnChunkIndex from every column chunk into curRowGroupIndex
            curRowGroupIndex.addColumnChunkIndexEntries(chunkIndexBuilder.build());
            // collect columnChunkStatistic into rowGroupStatistic
            curRowGroupStatistic.addColumnChunkStats(writer.getColumnChunkStat().build());
            // collect columnChunkEncoding
            curRowGroupEncoding.addColumnChunkEncodings(writer.getColumnChunkEncoding().build());
            // update file column statistic
            fileColStatRecorders[i].merge(writer.getColumnChunkStatRecorder());
            /* TODO: writer.reset() does not work for partitioned file writing, fix it later.
             * The possible reason is that: when the file is partitioned, the last stride of a row group
             * (a.k.a., partition) is likely not full (length < pixelsStride), thus if the writer is not
             * reset correctly, the strides of the next row group will not be written correctly.
             * We temporarily fix this problem by creating a new column writer for each row group.
             */
            // writer.reset();
            columnWriters[i] = newColumnWriter(children.get(i), pixelStride, encodingLevel, WRITER_ENDIAN);
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsProto.RowGroupFooter rowGroupFooter =
                PixelsProto.RowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .setRowGroupEncoding(curRowGroupEncoding.build())
                        .build();

        // write and flush row group footer
        byte[] footerBuffer = rowGroupFooter.toByteArray();
        // physicalWriter.prepare(footerBuffer.length);
        curRowGroupFooterOffset = bufWriter.writerIndex();
        bufWriter.writeBytes(footerBuffer, 0, footerBuffer.length);
        // System.out.println("curRowGroupFooterOffset: " + curRowGroupFooterOffset);
        // curRowGroupFooterOffset = physicalWriter.append(footerBuffer, 0, footerBuffer.length);
        writtenBytes += footerBuffer.length;
        // physicalWriter.flush();

        // update RowGroupInformation, and put it into rowGroupInfoList
        curRowGroupInfo.setFooterOffset(curRowGroupFooterOffset);
        curRowGroupInfo.setDataLength(rowGroupDataLength);
        curRowGroupInfo.setFooterLength(rowGroupFooter.getSerializedSize());
        curRowGroupInfo.setNumberOfRows(curRowGroupNumOfRows);
        if (partitioned) {
            PixelsProto.PartitionInformation.Builder partitionInfo =
                    PixelsProto.PartitionInformation.newBuilder();
            // partitionColumnIds has been checked to be present in the builder.
            partitionInfo.addAllColumnIds(partKeyColumnIds.orElse(null));
            partitionInfo.setHashValue(currHashValue);
            curRowGroupInfo.setPartitionInfo(partitionInfo.build());
        }
        rowGroupInfoList.add(curRowGroupInfo.build());
        // put curRowGroupStatistic into rowGroupStatisticList
        rowGroupStatisticList.add(curRowGroupStatistic.build());

        this.fileRowNum += curRowGroupNumOfRows;
        this.fileContentLength += rowGroupDataLength;
    }

    private void writeFileTail() throws IOException {
        PixelsProto.Footer footer;
        PixelsProto.PostScript postScript;

        // build Footer
        PixelsProto.Footer.Builder footerBuilder =
                PixelsProto.Footer.newBuilder();
        writeTypes(footerBuilder, schema);
        for (StatsRecorder recorder : fileColStatRecorders) {
            footerBuilder.addColumnStats(recorder.serialize().build());
        }
        for (PixelsProto.RowGroupInformation rowGroupInformation : rowGroupInfoList) {
            footerBuilder.addRowGroupInfos(rowGroupInformation);
        }
        for (PixelsProto.RowGroupStatistic rowGroupStatistic : rowGroupStatisticList) {
            footerBuilder.addRowGroupStats(rowGroupStatistic);
        }
        footer = footerBuilder.build();

        // build PostScript
        postScript = PixelsProto.PostScript.newBuilder()
                .setVersion(Constants.VERSION)
                .setContentLength(fileContentLength)
                .setNumberOfRows(fileRowNum)
                .setCompression(compressionKind)
                .setCompressionBlockSize(compressionBlockSize)
                .setPixelStride(pixelStride)
                .setWriterTimezone(timeZone.getDisplayName())
                .setPartitioned(partitioned)
                .setColumnChunkAlignment(chunkAlignment)
                .setMagic(Constants.MAGIC)
                .build();

        // build FileTail
        PixelsProto.FileTail fileTail =
                PixelsProto.FileTail.newBuilder()
                        .setFooter(footer)
                        .setPostscript(postScript)
                        .setFooterLength(footer.getSerializedSize())
                        .setPostscriptLength(postScript.getSerializedSize())
                        .build();

        // write and flush FileTail plus FileTail physical offset at the end of the file
        int fileTailLen = fileTail.getSerializedSize() + Long.BYTES;
        // physicalWriter.prepare(fileTailLen);
        long tailOffset = bufWriter.writerIndex();
        bufWriter.writeBytes(fileTail.toByteArray(), 0, fileTail.getSerializedSize());
        // System.out.println("tailOffset: " + tailOffset);
        // long tailOffset = physicalWriter.append(fileTail.toByteArray(), 0, fileTail.getSerializedSize());
        ByteBuffer tailOffsetBuffer = ByteBuffer.allocate(Long.BYTES);
        tailOffsetBuffer.putLong(tailOffset);
        tailOffsetBuffer.flip(); // Flip the buffer to change its position to the beginning
        bufWriter.writeBytes(tailOffsetBuffer);  // physicalWriter.append(tailOffsetBuffer);
        writtenBytes += fileTailLen;
        // physicalWriter.flush();
    }
}
