/**
 * ****************************************************************************
 *  Copyright(c) 2023 the original author Eduardo Iglesias Taylor.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  	 https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Contributors:
 *  	Eduardo Iglesias Taylor - initial API and implementation
 * *****************************************************************************
 */
package org.platkmframework.databasereader.core;

import java.sql.Connection;
import java.util.List;
import org.platkmframework.databasereader.model.Column;
import org.platkmframework.databasereader.model.Table;

/**
 *   Author:
 *     Eduardo Iglesias
 *   Contributors:
 *   	Eduardo Iglesias - initial API and implementation
 */
public interface IDatabaseReader {

    /**
     * getMetadata
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @return List
     */
    List<Table> getMetadata(Connection con, String catalog, String schema);

    /**
     * getTablePksContraints
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param table table
     * @return List
     */
    public List<String> getTablePksContraints(Connection con, String catalog, String schema, String table);

    /**
     * getTableColumnMetaData
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param tablename tablename
     * @return List
     */
    List<Column> getTableColumnMetaData(Connection con, String catalog, String schema, String tablename);
}
