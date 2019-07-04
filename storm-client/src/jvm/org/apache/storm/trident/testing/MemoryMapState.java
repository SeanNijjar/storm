/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.ITupleCollection;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.RemovableMapState;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.snapshot.Snapshottable;
import org.apache.storm.tuple.Values;

public class MemoryMapState<T> implements Snapshottable<T>, ITupleCollection, MapState<T>, RemovableMapState<T> {

    static ConcurrentHashMap<String, Map<List<Object>, Object>> _dbs = new ConcurrentHashMap<String, Map<List<Object>, Object>>();
    MemoryMapStateBacking<OpaqueValue> _backing;
    SnapshottableMap<T> _delegate;
    List<List<Object>> _removed = new ArrayList();
    Long _currTx = null;

    public MemoryMapState(String id) {
        _backing = new MemoryMapStateBacking(id);
        _delegate = new SnapshottableMap(OpaqueMap.build(_backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    }

    @Override
    public T update(ValueUpdater updater) {
        return _delegate.update(updater);
    }

    @Override
    public void set(T o) {
        _delegate.set(o);
    }

    @Override
    public T get() {
        return _delegate.get();
    }

    @Override
    public void beginCommit(Long txid) {
        _delegate.beginCommit(txid);
        if (txid == null || !txid.equals(_currTx)) {
            _backing.multiRemove(_removed);
        }
        _removed = new ArrayList();
        _currTx = txid;
    }

    @Override
    public void commit(Long txid) {
        _delegate.commit(txid);
    }

    @Override
    public Iterator<List<Object>> getTuples() {
        return _backing.getTuples();
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        return _delegate.multiUpdate(keys, updaters);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        _delegate.multiPut(keys, vals);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return _delegate.multiGet(keys);
    }

    @Override
    public void multiRemove(List<List<Object>> keys) {
        List nulls = new ArrayList();
        for (int i = 0; i < keys.size(); i++) {
            nulls.add(null);
        }
        // first just set the keys to null, then flag to remove them at beginning of next commit when we know the current and last value
        // are both null
        multiPut(keys, nulls);
        _removed.addAll(keys);
    }

    public static class Factory implements StateFactory {

        String _id;

        public Factory() {
            _id = UUID.randomUUID().toString();
        }

        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new MemoryMapState(_id + partitionIndex);
        }
    }

    static class MemoryMapStateBacking<T> implements IBackingMap<T>, ITupleCollection {

        Map<List<Object>, T> db;
        Long currTx;

        public MemoryMapStateBacking(String id) {
            if (!_dbs.containsKey(id)) {
                _dbs.put(id, new HashMap());
            }
            this.db = (Map<List<Object>, T>) _dbs.get(id);
        }

        public static void clearAll() {
            _dbs.clear();
        }

        public void multiRemove(List<List<Object>> keys) {
            for (List<Object> key : keys) {
                db.remove(key);
            }
        }

        @Override
        public List<T> multiGet(List<List<Object>> keys) {
            List<T> ret = new ArrayList();
            for (List<Object> key : keys) {
                ret.add(db.get(key));
            }
            return ret;
        }

        @Override
        public void multiPut(List<List<Object>> keys, List<T> vals) {
            for (int i = 0; i < keys.size(); i++) {
                List<Object> key = keys.get(i);
                T val = vals.get(i);
                db.put(key, val);
            }
        }

        @Override
        public Iterator<List<Object>> getTuples() {
            return new Iterator<List<Object>>() {

                private Iterator<Map.Entry<List<Object>, T>> it = db.entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public List<Object> next() {
                    Map.Entry<List<Object>, T> e = it.next();
                    List<Object> ret = new ArrayList<Object>();
                    ret.addAll(e.getKey());
                    ret.add(((OpaqueValue) e.getValue()).getCurr());
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }
    }
}
