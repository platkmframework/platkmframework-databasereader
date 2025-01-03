/*******************************************************************************
 * Copyright(c) 2023 the original author Eduardo Iglesias Taylor.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	 https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 * 	Eduardo Iglesias Taylor - initial API and implementation
 *******************************************************************************/
package org.platkmframework.databasereader.core;

import java.util.List;

import org.platkmframework.databasereader.model.FkContraint;
import org.platkmframework.databasereader.model.Table;
 



/**
 *   Author: 
 *     Eduardo Iglesias
 *   Contributors: 
 *   	Eduardo Iglesias - initial API and implementation
 * Created on 04-jun-2013, 13:02:05
 */
public class DataBase {

    private String name;
    private List<Table> tables; 

    DataBase(String catalogo) {
        this.name = catalogo;
    }
 

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * 
     * @param tableName
     * @return 
     */
    public List<FkContraint> getFkContraint(final String tableName){
    
        List<FkContraint>  listFkContraint = null;
        
        for (Table table : tables) {
            if (table.getName().equals(tableName)){
                listFkContraint = table.getFkContraint();
            }
        }
        
        return listFkContraint;
    }
    
}
