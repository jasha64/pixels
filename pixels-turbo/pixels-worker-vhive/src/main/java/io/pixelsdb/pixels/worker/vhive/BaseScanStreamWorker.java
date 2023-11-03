/*
 * Copyright 2022-2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.*;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class BaseScanStreamWorker extends Worker<ScanInput, ScanOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;
//    private PixelsReader pixelsReader;
//    private AtomicReference<PixelsWriter> pixelsWriter;

    public BaseScanStreamWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public ScanOutput process(ScanInput event)  // change this to void or async? The next-level workers need the output info before the current worker finishes.
    {
        ScanOutput scanOutput = new ScanOutput();
        long startTime = System.currentTimeMillis();
        scanOutput.setStartTimeMs(startTime);
        scanOutput.setRequestId(context.getRequestId());
        scanOutput.setSuccessful(true);
        scanOutput.setErrorMessage("");
        workerMetrics.clear();

        try {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            requireNonNull(event.getTableInfo(), "even.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            boolean[] scanProjection = requireNonNull(event.getScanProjection(),
                    "event.scanProjection is null");
            boolean partialAggregationPresent = event.isPartialAggregationPresent();

            String outputFolder = event.getOutput().getPath();
            StorageInfo outputStorageInfo = event.getOutput().getStorageInfo();
            // outputFolder will be the hashValue in streaming mode?
            boolean encoding = event.getOutput().isEncoding();

            StreamWorkerCommon.initStorage(inputStorageInfo, false);  // maybe initialize a PixelsReader because we need to keep a consistent reader for the same endpoint (folder?) across all paths (files?) under it (assume all the input splits are from the same endpoint)
            StreamWorkerCommon.initStorage(outputStorageInfo, true);

            String[] includeCols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);

            Aggregator aggregator;  // partition, scan, chainjoin 可能需要读S3上的表 (代码里标注BaseTable == true的)
            if (partialAggregationPresent)
            {
                logger.debug("start get output schema, outputStorageInfo scheme = " + outputStorageInfo.getScheme() + ", region = " + outputStorageInfo.getRegion());
                TypeDescription inputSchema = StreamWorkerCommon.getSchemaFromSplits(StreamWorkerCommon.getStorage(inputStorageInfo.getScheme()),
                        inputSplits);  // XXX: The better way is to include the schema in the header of the first rowBatch
                // maybe need to call a readBatch(0), which ensures the header is parsed
                inputSchema = StreamWorkerCommon.getResultSchema(inputSchema, includeCols);

                PartialAggregationInfo partialAggregationInfo = event.getPartialAggregationInfo();
                requireNonNull(partialAggregationInfo, "event.partialAggregationInfo is null");
                boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
                Arrays.fill(groupKeyProjection, true);

                aggregator = new Aggregator(StreamWorkerCommon.rowBatchSize, inputSchema,
                        partialAggregationInfo.getGroupKeyColumnAlias(),
                        partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
                        partialAggregationInfo.getAggregateColumnIds(),
                        partialAggregationInfo.getResultColumnAlias(),
                        partialAggregationInfo.getResultColumnTypes(),
                        partialAggregationInfo.getFunctionTypes(),
                        partialAggregationInfo.isPartition(),
                        partialAggregationInfo.getNumPartition());
                if (outputFolder.endsWith("_0"))  // Only one worker is responsible for passing the schema to the next level.
                    StreamWorkerCommon.passSchemaToNextLevel(aggregator.getOutputSchema(), outputStorageInfo);
            }
            else
            {
                aggregator = null;
            }

            int outputId = 0;
            logger.info("start scan and aggregate");
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();  // to be pipelined
                String outputPath = outputFolder + "scan_" + outputId++;

                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = scanFile(transId, scanInputs, includeCols, inputStorageInfo.getScheme(),
                                scanProjection, filter, outputPath, encoding, outputStorageInfo.getScheme(),
                                partialAggregationPresent, aggregator);
                        // Currently, ... Can change String outputPath to some HTTP URL (and include the hashValue (scanId) at the end of the URL). Or maybe include the hashValue in the HTTP header (so change the outputPath from String to HttpRequest)
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(outputPath, rowGroupNum);
                        }
                    }
                    catch (Throwable e)
                    {
                        throw new WorkerException("error during scan", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of scan", e);
            }
            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }
            // todo:
            // We cannot await termination here. We should just start the next level workers in the operator - in this way
            //  is the entire worker flow further pipelined. Instead, we should await termination in the end of this worker.
            // (即边写边发，不阻塞writer地发buffer)
            // Or alternatively, change AggregationOperator to not block on `executePrev()`, so that the worker thread
            //  remains running to manage these async threads.

            if (partialAggregationPresent)
            {
                logger.info("start write aggregation result");
                String outputPath = event.getOutput().getPath();
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                logger.debug("start get writer " + outputStorageInfo.getScheme());
                PixelsWriter pixelsWriter =  // outputStorageInfo.getScheme() == mock ? this.pixelsWriter :
                        StreamWorkerCommon.getWriter(aggregator.getOutputSchema(),
                        StreamWorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                        aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                aggregator.writeAggrOutput(pixelsWriter);
                pixelsWriter.close();
                workerMetrics.addOutputCostNs(writeCostTimer.stop());
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                scanOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            }

            // Move here?
//            try
//            {
//                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
//            } catch (InterruptedException e)
//            {
//                throw new WorkerException("interrupted while waiting for the termination of scan", e);
//            }
//            if (exceptionHandler.hasException())
//            {
//                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
//            }

            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(scanOutput, workerMetrics);
            return scanOutput;
        } catch (Throwable e)
        {
            logger.error("error during scan", e);
            scanOutput.setSuccessful(false);
            scanOutput.setErrorMessage(e.getMessage());
            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
             return scanOutput;
        }
    }

    private int scanFile(long transId, List<InputInfo> scanInputs, String[] columnsToRead, Storage.Scheme inputScheme,
                         boolean[] scanProjection, TableScanFilter filter, String outputEndpoint,  // host:port
                         boolean encoding,
                         Storage.Scheme outputScheme, boolean partialAggregate, Aggregator aggregator)
    {
        PixelsWriter pixelsWriter = null;
        // We can just throw out any exception because Pixels handles all exceptions centrally.
        Scanner scanner = null;
        if (partialAggregate)
        {
            requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        }
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;

        // Because we have ample free ports, we can start a PixelsReader for each file.
        // // Under our current setting, in streaming mode one worker uses only one PixelsReader for all multiple files because
        // //  they are all from the same endpoint. Or is it?
        readCostTimer.start();
//        PixelsReader pixelsReader = StreamWorkerCommon.getReader(scanInputs.get(0).getPath());
        readCostTimer.stop();
        // // Given that we can only alloc one port per worker, we can only start one PixelsReader instance per worker,
        // //  so we have to make the PixelsReader thread-safe. However, for demo we can allow multiple ports per worker,
        // //  and specifically, one port per endpoint.

        for (InputInfo inputInfo : scanInputs)
        {
            logger.debug("creating a new pixelsReader on endpoint " + inputInfo.getPath());
            readCostTimer.start();
            try (PixelsReader pixelsReader = StreamWorkerCommon.getReader(inputScheme, inputInfo.getPath()))
            {
                readCostTimer.stop();
                // if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum()) continue;
                PixelsReaderOption option = StreamWorkerCommon.getReaderOption(transId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                if (scanner == null)
                {
                    scanner = new Scanner(StreamWorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter);
                }
                if (pixelsWriter == null && !partialAggregate)
                {
                    logger.debug("creating a new pixelsWriter on endpoint " + outputEndpoint);
                    writeCostTimer.start();
                    pixelsWriter = StreamWorkerCommon.getWriter(scanner.getOutputSchema(), StreamWorkerCommon.getStorage(outputScheme),
                            outputEndpoint, encoding, false, null);
                    writeCostTimer.stop();
                }

                computeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
//                    logger.debug("recordReader readBatch on endpoint " + inputInfo.getPath());
                    if (rowBatch.size > 0)
                    {
                        if (partialAggregate)
                        {
                            aggregator.aggregate(rowBatch);
                            // aggregator.writeAggrOutput();  // Can write aggregate output every time
                        } else
                        {
                            pixelsWriter.addRowBatch(rowBatch);
                            // Our PixelsWriterStreamImpl includes the pipelining logic
                        }
                    }
                } while (!rowBatch.endOfFile);
                logger.debug("Read complete on endpoint " + inputInfo.getPath());
                computeCostTimer.stop();
                computeCostTimer.minus(recordReader.getReadTimeNanos());
                readCostTimer.add(recordReader.getReadTimeNanos());
                readBytes += recordReader.getCompletedBytes();
                numReadRequests += recordReader.getNumReadRequests();
            } catch (Throwable e)
            {
                throw new WorkerException("failed to scan the endpoint '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }

        // Finished scanning all the files in the split.
        try {
            int numRowGroup = 0;
            if (pixelsWriter != null)
            {
                // This is a pure scan without aggregation, compute time is the file writing time.
                writeCostTimer.add(computeCostTimer.getElapsedNs());
                writeCostTimer.start();
                pixelsWriter.close();
                writeCostTimer.stop();
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                workerMetrics.addOutputCostNs(writeCostTimer.getElapsedNs());
                numRowGroup = pixelsWriter.getNumRowGroup();
            }
            else
            {
                workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
            }
            workerMetrics.addReadBytes(readBytes);
            workerMetrics.addNumReadRequests(numReadRequests);
            workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
            return numRowGroup;
        } catch (Throwable e)
        {
            throw new WorkerException(
                    "failed finish writing and closing the output endpoint '" + outputEndpoint + "'", e);
        }
    }
}
