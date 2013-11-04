/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.storage;

import java.util.List;
import java.util.Properties;

import poke.server.vo.FileInfo;
import eye.Comm.Document;
import eye.Comm.NameSpace;
import eye.Comm.Request;

public interface Storage {

    void init(Properties cfg);

    void release();

    NameSpace getNameSpaceInfo(long spaceId);

    List<NameSpace> findNameSpaces(NameSpace criteria);

    NameSpace createNameSpace(NameSpace space);

    boolean removeNameSpace(long spaceId);

    // boolean addDocument(String namespace, Document doc, String databaseName);

    // boolean addDocument(Request request, String tableName, String filePath);

    boolean addDocument(Request request, String filePath, String serverPort);

    boolean removeDocument(String fileName, long docID);

    String removeDocumentfromDB(String fileName);

    boolean updateDocument(String namespace, Document doc);

    // inserted on oct 29
    boolean updateDocument(Request request, String filePath);

    // inserted on nov2
    FileInfo findDocument(Request request, String fileName);

    List<Document> findDocuments(String namespace, Document criteria);
}
