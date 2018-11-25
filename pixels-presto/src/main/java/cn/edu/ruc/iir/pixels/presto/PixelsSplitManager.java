/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsMetadataProxy;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import cn.edu.ruc.iir.pixels.presto.split.*;
import com.alibaba.fastjson.JSON;
import com.coreos.jetcd.data.KeyValue;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsSplitManager
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 19:16
 **/
public class PixelsSplitManager
        implements ConnectorSplitManager {
    private final Logger log = Logger.get(PixelsSplitManager.class);
    private final String connectorId;
    private final FSFactory fsFactory;
    private final PixelsMetadataProxy metadataProxy;

    @Inject
    public PixelsSplitManager(PixelsConnectorId connectorId, PixelsMetadataProxy metadataProxy, PixelsPrestoConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.fsFactory = requireNonNull(config.getFsFactory(), "fsFactory is null");
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle tableLayout,
                                          SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PixelsTableLayoutHandle layoutHandle = (PixelsTableLayoutHandle) tableLayout;
        PixelsTableHandle tableHandle = layoutHandle.getTable();

        TupleDomain<PixelsColumnHandle> constraint = layoutHandle.getConstraint()
                .transform(PixelsColumnHandle.class::cast);
        Set<PixelsColumnHandle> desiredColumns = layoutHandle.getDesiredColumns().stream().map(PixelsColumnHandle.class::cast)
                .collect(toSet());

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        List<Layout> layouts;
        try
        {
            layouts = metadataProxy.getDataLayouts(tableHandle.getSchemaName(),
                    tableHandle.getTableName());
        }
        catch (MetadataException e)
        {
            throw new PrestoException(PIXELS_METASTORE_ERROR, e);
        }

        List<ConnectorSplit> pixelsSplits = new ArrayList<>();
        for (Layout layout : layouts)
        {
            // get index
            int version = layout.getVersion();
            IndexEntry indexEntry = new IndexEntry(schemaName, tableName);
            Inverted index = (Inverted) IndexFactory.Instance().getIndex(indexEntry);
            Order order = JSON.parseObject(layout.getOrder(), Order.class);
            Splits splits = JSON.parseObject(layout.getSplits(), Splits.class);
            if (index == null)
            {
                log.info("action null");
                index = getInverted(order, splits, indexEntry);
            }
            else
            {
                log.info("action not null");
                int indexVersion = index.getVersion();
                if (indexVersion < version) {
                    log.info("action not null update");
                    index = getInverted(order, splits, indexEntry);
                }
            }
            // get split size
            ColumnSet columnSet = new ColumnSet();
            for (PixelsColumnHandle column : desiredColumns) {
                log.info(column.getColumnName());
                columnSet.addColumn(column.getColumnName());
            }
            AccessPattern bestPattern = index.search(columnSet);
            log.info("bestPattern: " + bestPattern.toString());
            int splitSize = bestPattern.getSplitSize();
            int rowGroupNum = splits.getNumRowGroupInBlock();

            ConfigFactory configFactory = ConfigFactory.Instance();
            String cache = configFactory.getProperty("cache.enabled");
            boolean cacheEnabled = cache != null ? Boolean.parseBoolean(cache) : false;
            if(cacheEnabled)
            {
                String cacheVersion;
                EtcdUtil etcdUtil = EtcdUtil.Instance();
                KeyValue keyValue = etcdUtil.getKeyValue("cache_version");
                if(keyValue != null)
                {
                    // 1. get version
                    cacheVersion = keyValue.getValue().toStringUtf8();
                    log.info("cache version: " + cacheVersion);
                    // 2. get files of each node
                    List<KeyValue> nodeFiles = etcdUtil.getKeyValuesByPrefix("location_" + cacheVersion);
                    if(nodeFiles.size() > 0)
                    {
                        Map<String, String> fileToNodeMap = new HashMap<>();
                        for (KeyValue kv : nodeFiles)
                        {
                            String node = kv.getKey().toStringUtf8().split("_")[2];
                            String[] files = kv.getValue().toStringUtf8().split(";");
                            for(String file : files)
                            {
                                fileToNodeMap.put(file, node);
                                log.info("cache location: {file='" + file + "', node='" + node + "'");
                            }
                        }
                        // 3. add splits in orderPath
                        try
                        {
                            for (Path file : fsFactory.listFiles(layout.getOrderPath()))
                            {
                                String hdfsFile = file.toString();
                                String node = fileToNodeMap.get(hdfsFile);
                                List<HostAddress> hostAddresses  = fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE, node);
                                PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                        tableHandle.getSchemaName(), tableHandle.getTableName(),
                                        hdfsFile, 0, 1,
                                        cacheEnabled, hostAddresses, order.getColumnOrder(), constraint);
                                pixelsSplits.add(pixelsSplit);
                            }
                        } catch (FSException e)
                        {
                            throw new PrestoException(PIXELS_HDFS_FILE_ERROR, e);
                        }
                        // 4. add splits in compactionPath
                        int curFileRGIdx;
                        try
                        {
                            for (Path file : fsFactory.listFiles(layout.getCompactPath()))
                            {
                                curFileRGIdx = 0;
                                while (curFileRGIdx < rowGroupNum)
                                {
                                    String hdfsFile = file.toString();
                                    String node = fileToNodeMap.get(hdfsFile);
                                    List<HostAddress> hostAddresses  = fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE, node);
                                    PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                            tableHandle.getSchemaName(), tableHandle.getTableName(),
                                            hdfsFile, curFileRGIdx, splitSize,
                                            cacheEnabled, hostAddresses, order.getColumnOrder(), constraint);
                                    pixelsSplits.add(pixelsSplit);
                                    curFileRGIdx += splitSize;
                                }
                            }
                        } catch (FSException e)
                        {
                            throw new PrestoException(PIXELS_HDFS_FILE_ERROR, e);
                        }
                    }
                    else
                    {
                        log.info("Get caching files error when version is " + cacheVersion);
                        System.exit(-1);
                    }
                }
                else
                {
                    log.info("Get caching version error. ");
                    System.exit(-1);
                }
            }
            else
            {
                // add splits in orderPath
                try
                {
                    for (Path file : fsFactory.listFiles(layout.getOrderPath()))
                    {
                        PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                tableHandle.getSchemaName(), tableHandle.getTableName(),
                                file.toString(), 0, 1,
                                cacheEnabled, fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE), order.getColumnOrder(), constraint);
                        pixelsSplits.add(pixelsSplit);
                    }
                } catch (FSException e)
                {
                    throw new PrestoException(PIXELS_HDFS_FILE_ERROR, e);
                }
                // add splits in compactionPath
                int curFileRGIdx;
                try
                {
                    List<Path> filePaths = fsFactory.listFiles(layout.getCompactPath());
                    Balancer balancer = new Balancer();

                    for (Path path : filePaths)
                    {
                        List<HostAddress> addresses = fsFactory.getBlockLocations(path, 0, Long.MAX_VALUE);
                        balancer.put(addresses.get(0), path);
                    }
                    balancer.balance();
                    for (Path path : fsFactory.listFiles(layout.getCompactPath()))
                    {
                        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
                        builder.add(balancer.get(path));
                        curFileRGIdx = 0;
                        while (curFileRGIdx < rowGroupNum)
                        {
                            PixelsSplit pixelsSplit = new PixelsSplit(connectorId,
                                    tableHandle.getSchemaName(), tableHandle.getTableName(),
                                    path.toString(), curFileRGIdx, splitSize,
                                    cacheEnabled, builder.build(), order.getColumnOrder(), constraint);
                            pixelsSplits.add(pixelsSplit);
                            curFileRGIdx += splitSize;
                        }
                    }
                } catch (FSException e)
                {
                    throw new PrestoException(PIXELS_HDFS_FILE_ERROR, e);
                }
            }
        }

        log.info("pixelsSplits: " + pixelsSplits.size());
        log.info("=====begin to shuffle====");
        Collections.shuffle(pixelsSplits);

        return new FixedSplitSource(pixelsSplits);
    }

    private Inverted getInverted(Order order, Splits splits, IndexEntry indexEntry) {
        List<String> columnOrder = order.getColumnOrder();
        Inverted index;
        try {
            index = new Inverted(columnOrder, AccessPattern.buildPatterns(columnOrder, splits), splits.getNumRowGroupInBlock());
            IndexFactory.Instance().cacheIndex(indexEntry, index);
        } catch (IOException e) {
            log.info("getInverted error: " + e.getMessage());
            throw new PrestoException(PIXELS_INVERTED_INDEX_ERROR, e);
        }
        return index;
    }

    public static class Balancer
    {
        private int totalCount = 0;
        private Map<HostAddress, Integer> nodeCounters = new HashMap<>();
        private Map<Path, HostAddress> pathToAddress = new HashMap<>();

        public void put (HostAddress address, Path path)
        {
            if (this.nodeCounters.containsKey(address))
            {
                this.nodeCounters.put(address, this.nodeCounters.get(address)+1);
            }
            else
            {
                this.nodeCounters.put(address, 1);
            }
            this.pathToAddress.put(path, address);
            this.totalCount++;
        }

        public HostAddress get (Path path)
        {
            return this.pathToAddress.get(path);
        }

        public void balance ()
        {
            int ceil = (int) Math.ceil((double)this.totalCount / (double)this.nodeCounters.size());
            int floor = (int) Math.floor((double)this.totalCount / (double)this.nodeCounters.size());

            List<HostAddress> peak = new ArrayList<>();
            List<HostAddress> valley = new ArrayList<>();

            for (Map.Entry<HostAddress, Integer> entry : this.nodeCounters.entrySet())
            {
                if (entry.getValue() > ceil)
                {
                    peak.add(entry.getKey());
                }

                if (entry.getValue() < floor)
                {
                    valley.add(entry.getKey());
                }
            }

            boolean balanced = false;

            while (balanced == false)
            {
                if (peak.isEmpty() || valley.isEmpty())
                {
                    break;
                }
                HostAddress peakAddress = peak.get(0);
                HostAddress valleyAddress = valley.get(0);
                if (this.nodeCounters.get(peakAddress) <= ceil)
                {
                    peak.remove(peakAddress);
                }
                if (this.nodeCounters.get(valleyAddress) >= floor)
                {
                    valley.remove(valleyAddress);
                }
                this.nodeCounters.put(peakAddress, this.nodeCounters.get(peakAddress)-1);
                this.nodeCounters.put(valleyAddress, this.nodeCounters.get(valleyAddress)+1);

                for (Map.Entry<Path, HostAddress> entry : this.pathToAddress.entrySet())
                {
                    if (entry.getValue().equals(peakAddress))
                    {
                        this.pathToAddress.put(entry.getKey(), valleyAddress);
                        break;
                    }
                }

                balanced = this.isBalanced();
            }
        }

        public boolean isBalanced ()
        {
            int ceil = (int) Math.ceil((double)this.totalCount / (double)this.nodeCounters.size());
            int floor = (int) Math.floor((double)this.totalCount / (double)this.nodeCounters.size());

            boolean balanced = true;
            for (Map.Entry<HostAddress, Integer> entry : this.nodeCounters.entrySet())
            {
                if (entry.getValue() > ceil || entry.getValue() < floor)
                {
                    balanced = false;
                }
            }

            return balanced;
        }
    }
}