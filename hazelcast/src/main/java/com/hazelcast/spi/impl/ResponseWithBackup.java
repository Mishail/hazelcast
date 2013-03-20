/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.*;

import java.io.IOException;

/**
 * @mdogan 10/1/12
 */

final class ResponseWithBackup extends AbstractOperation implements BackupOperation, KeyBasedOperation, IdentifiedDataSerializable {

    private int keyHash;

    private Operation backupOp;

    private Object responseObject;

    private transient Response response;

    public ResponseWithBackup() {
    }

    public ResponseWithBackup(Operation backupOp, Object response) {
        if (!(backupOp instanceof BackupOperation)) {
            throw new IllegalArgumentException("Backup operation should be instance of BackupOperation!");
        }
        this.backupOp = backupOp;
        this.responseObject = response;
        this.keyHash = backupOp instanceof KeyBasedOperation ? ((KeyBasedOperation) backupOp).getKeyHash() : 0;
    }

    public void prepare() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        if (backupOp != null) {
            backupOp.setNodeEngine(nodeEngine)
                    .setCallerAddress(getCallerAddress())
                    .setCallerUuid(getCallerUuid())
                    .setConnection(getConnection())
                    .setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler())
//                    .setResponseHandler(ResponseHandlerFactory.createErrorLoggingResponseHandler(nodeEngine.getLogger(backupOp.getClass())))
            ;
            OperationAccessor.setStartTime(backupOp, getStartTime());
        }

        response = responseObject instanceof Response ? (Response) responseObject : new Response(responseObject);
        response.setNodeEngine(nodeEngine)
                .setCallerAddress(getCallerAddress())
                .setCallerUuid(getCallerUuid())
                .setConnection(getConnection());
        OperationAccessor.setCallId(response, getCallId());
    }

    public Operation getBackupOp() {
        return backupOp;
    }

    public Response getResponse() {
        return response;
    }

    //    public void beforeRun() throws Exception {
//        final NodeEngine nodeEngine = getNodeEngine();
//        if (backupOp != null) {
//            backupOp.setNodeEngine(nodeEngine)
//                    .setCallerAddress(getCallerAddress())
//                    .setCallerUuid(getCallerUuid())
//                    .setConnection(getConnection());
//            OperationAccessor.setStartTime(backupOp, getStartTime());
//        }
//
//        response = responseObject instanceof Response ? (Response) responseObject : new Response(responseObject);
//        response.setNodeEngine(nodeEngine)
//                .setCallerAddress(getCallerAddress())
//                .setCallerUuid(getCallerUuid())
//                .setConnection(getConnection());
//        OperationAccessor.setCallId(response, getCallId());
//    }


    public void beforeRun() throws Exception {
        throw new UnsupportedOperationException();
    }

    public void run() throws Exception {
        throw new UnsupportedOperationException();
//        if (backupOp != null) {
//            try {
//                backupOp.beforeRun();
//                backupOp.run();
//                backupOp.afterRun();
//            } catch (Throwable e) {
//                getLogger().log(Level.WARNING, "While executing backup operation within ResponseWithBackup: " + e.getMessage(), e);
//            }
//        }
//        try {
//            response.beforeRun();
//            response.run();
//            response.afterRun();
//        } catch (Throwable e) {
//            getLogger().log(Level.SEVERE, "While processing response...", e);
//        }
    }

    private ILogger getLogger() {
        return getNodeEngine().getLogger(getClass());
    }

    public void afterRun() throws Exception {
        throw new UnsupportedOperationException();
    }

    public int getKeyHash() {
        return keyHash;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(backupOp);
        final boolean isData = responseObject instanceof Data;
        out.writeBoolean(isData);
        if (isData) {
            ((Data) responseObject).writeData(out);
        } else {
            out.writeObject(responseObject);
        }
        out.writeInt(keyHash);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        backupOp = in.readObject();
        final boolean isData = in.readBoolean();
        if (isData) {
            Data data = new Data();
            data.readData(in);
            responseObject = data;
        } else {
            responseObject = in.readObject();
        }
        keyHash = in.readInt();
    }

    public int getId() {
        return DataSerializerSpiHook.MULTI_RESPONSE;
    }
}
