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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.platkmframework.databasereader.model.Column;
import org.platkmframework.databasereader.model.FkContraint;
import org.platkmframework.databasereader.model.ImportedKey;
import org.platkmframework.databasereader.model.IndexContraint;
import org.platkmframework.databasereader.model.PkContraint;
import org.platkmframework.databasereader.model.Table;
import org.platkmframework.util.Util;

/**
 *   Author:
 *     Eduardo Iglesias
 *   Contributors:
 *   	Eduardo Iglesias - initial API and implementation
 * Las contraint son:
 *   - llave primaria
 *   - llave foranea
 *   - unique
 *   - check
 *
 * Indices
 *   - tienen una estructura con informacion particular
 *     de cada tabla
 */
public class DatabaseReader implements IDatabaseReader {

    /**
     * Atributo logger
     */
    private static Logger logger = LoggerFactory.getLogger(DatabaseReader.class);

    /**
     * nombre de la tabla de la cual se tiene llaves foraneas
     */
    public static final String PKTABLE_NAME = "PKTABLE_NAME";

    /**
     * nombre de la columna de la tabla de la cual se hace referencia,
     * seria la llave primaria de la tabla de la cual se hace foreing key
     */
    public static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";

    /**
     * nombre de la contraing de la tabla que tiene la foreing key
     */
    public static final String FK_NAME = "FK_NAME";

    /**
     * nombre de la tabla que tiene llaves foraneas
     */
    public static final String FKTABLE_NAME = "FKTABLE_NAME";

    /**
     * nombre de la columna foreing key
     */
    public static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";

    /**
     * nombre de la contraing de llave primaria de la tabla cuyo
     * campo es usado en otra tabla como foreing key.
     */
    public static final String PK_NAME = "PK_NAME";

    /**
     * operacion a realizar cdo la tabla principal
     * borre un campo
     */
    public static final String DELETE_RULE = "DELETE_RULE";

    /**
     * operacion a realizar cdo la tabla principal
     * actualice un campo
     */
    public static final String UPDATE_RULE = "DELETE_RULE";

    /**
     * Atributo COLUMN_NAME
     */
    public static final String COLUMN_NAME = "COLUMN_NAME";

    /**
     * Atributo con
     */
    Connection con = null;

    /**
     * Atributo excludedTables
     */
    List<String> excludedTables = null;

    /**
     * Atributo log
     */
    private String log;

    /**
     * Constructor DatabaseReader
     */
    public DatabaseReader() {
        this.con = null;
    }

    /**
     * Constructor DatabaseReader
     * @param con con
     */
    public DatabaseReader(Connection con) {
        this.con = con;
    }

    /**
     * Constructor DatabaseReader
     * @param pexcludedTables pexcludedTables
     */
    public DatabaseReader(List<String> pexcludedTables) {
        this(null, pexcludedTables);
    }

    /**
     * Constructor DatabaseReader
     * @param con con
     * @param pexcludedTables pexcludedTables
     */
    public DatabaseReader(Connection con, List<String> pexcludedTables) {
        this(con);
        log = "";
        if (pexcludedTables != null)
            pexcludedTables.replaceAll(String::toUpperCase);
        this.excludedTables = pexcludedTables;
    }

    /**
     * processDatabase
     * @param catalogo catalogo
     * @param esquema esquema
     * @param tabla tabla
     * @param types types
     * @param listSelectedTablesNames listSelectedTablesNames
     * @return DataBase
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws SQLException SQLException
     * @throws InstantiationException InstantiationException
     * @throws DataBaseReaderException DataBaseReaderException
     * @throws IllegalAccessException IllegalAccessException
     * @throws IllegalArgumentException IllegalArgumentException
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws SecurityException SecurityException
     */
    public DataBase processDatabase(String catalogo, String esquema, String tabla, String[] types, List<String> listSelectedTablesNames) throws ClassNotFoundException, SQLException, InstantiationException, DataBaseReaderException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        progressInfo("Estableciendo conexion a base de datos...");
        DataBase dataBase = new DataBase(catalogo);
        progressInfo("Conexion establecida...");
        List<Table> listTablesName;
        if (listSelectedTablesNames != null && !listSelectedTablesNames.isEmpty()) {
            listTablesName = new ArrayList<>();
            String tableType = "TABLE";
            for (int i = 0; i < listSelectedTablesNames.size(); i++) {
                String tableName = listSelectedTablesNames.get(i);
                Table table = new Table();
                table.setId(Util.generateId(255));
                table.setName(tableName);
                table.setType(tableType);
                listTablesName.add(table);
            }
        } else
            listTablesName = readTablesName(con, catalogo, esquema, tabla, types, excludedTables);
        progressInfo("Comienzo de lectura de la informacion...");
        List<Table> tables = tablesProcess(listTablesName, catalogo);
        progressInfo("Lectura de la informacion finalizada...");
        dataBase.setTables(tables);
        dataBase.setName(con.getCatalog());
        progressInfo("Cerrando la conexion...");
        con.close();
        progressInfo("Proceso de lectura finalizado...");
        return dataBase;
    }

    /**
     * processDatabase
     * @param user user
     * @param password password
     * @param url url
     * @param driver driver
     * @param catalogo catalogo
     * @param esquema esquema
     * @param tabla tabla
     * @param types types
     * @param listSelectedTablesNames listSelectedTablesNames
     * @return DataBase
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws SQLException SQLException
     * @throws InstantiationException InstantiationException
     * @throws DataBaseReaderException DataBaseReaderException
     * @throws IllegalAccessException IllegalAccessException
     * @throws IllegalArgumentException IllegalArgumentException
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws SecurityException SecurityException
     */
    public DataBase processDatabase(String user, String password, String url, String driver, String catalogo, String esquema, String tabla, String[] types, List<String> listSelectedTablesNames) throws ClassNotFoundException, SQLException, InstantiationException, DataBaseReaderException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        progressInfo("Estableciendo conexion a base de datos...");
        DataBase dataBase = new DataBase(catalogo);
        Class.forName(driver).getDeclaredConstructor().newInstance();
        con = DriverManager.getConnection(url, user, password);
        progressInfo("Conexion establecida...");
        List<Table> listTablesName;
        if (listSelectedTablesNames != null && listSelectedTablesNames.size() > 0) {
            listTablesName = new ArrayList<>();
            String tableType = "TABLE";
            Table table;
            for (int i = 0; i < listSelectedTablesNames.size(); i++) {
                String tableName = listSelectedTablesNames.get(i);
                table = new Table();
                table.setId(Util.generateId(255));
                table.setName(tableName);
                table.setType(tableType);
                listTablesName.add(table);
            }
        } else
            listTablesName = readTablesName(con, catalogo, esquema, tabla, types, excludedTables);
        progressInfo("Comienzo de lectura de la informacion...");
        List<Table> tables = tablesProcess(listTablesName, catalogo);
        progressInfo("Lectura de la informacion finalizada...");
        dataBase.setTables(tables);
        dataBase.setName(con.getCatalog());
        progressInfo("Cerrando la conexion...");
        con.close();
        progressInfo("Proceso de lectura finalizado...");
        return dataBase;
    }

    /**
     * readTables
     * @param catalog catalog
     * @param schemaPattern schemaPattern
     * @param tabla tabla
     * @param types types
     * @return List
     * @throws InstantiationException InstantiationException
     * @throws IllegalAccessException IllegalAccessException
     * @throws ClassNotFoundException ClassNotFoundException
     * @throws SQLException SQLException
     * @throws IllegalArgumentException IllegalArgumentException
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     * @throws SecurityException SecurityException
     */
    public List<Table> readTables(String catalog, String schemaPattern, String tabla, String[] types) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        progressInfo("Estableciendo conexion a base de datos...");
        progressInfo("Conexion establecida...");
        progressInfo("Comienzo de lectura de la informacion...");
        List<Table> tables = readBasicTableInfo(con, catalog, schemaPattern, tabla, types);
        progressInfo("Proceso de lectura finalizado...");
        return tables;
    }

    /**
     * tablesProcess
     * @param listTablesName listTablesName
     * @param catalogo catalogo
     * @return List
     * @throws DataBaseReaderException DataBaseReaderException
     */
    protected List<Table> tablesProcess(List<Table> listTablesName, String catalogo) throws DataBaseReaderException {
        List<Table> tableList = new ArrayList<>();
        try {
            progressInfo("Loading tables");
            DatabaseMetaData databaseMetaData = con.getMetaData();
            int total = listTablesName.size();
            int proccessed = 0;
            for (Table table : listTablesName) {
                //progress information
                progressInfo("Table: " + table.getName());
                progressInfo("Processed " + proccessed + " of " + total);
                PkContraint pkContraint = tablePks(con, table.getName());
                List<IndexContraint> listIndexContraint = indexFields(con, table.getName(), con.getCatalog());
                //se busca la informacion por medio de una sentencia sql nativa
                //q no devuelve nada, pero lo que hace falta es la estructura
                //o la metadata. Todas las conexion son a bases de datos relacionales
                //con que tengan como base SQL Natvie
                Statement st = con.createStatement();
                //ResultSet rsTable1 = con.getMetaData().getTypeInfo();
                //                while (rsTable1.next()) {
                //                   System.out.println(rsTable1.getString(1) + "-" +  rsTable1.getString(10));
                //                }
                //ResultSet rsTable = st.executeQuery("SELECT * FROM " + (StringUtils.isEmpty(con.getCatalog())?tableName:con.getSchema()Catalog() + "." + tableName )+ " WHERE 1=2");
                ResultSet rsTable = st.executeQuery("SELECT * FROM " + table.getName() + " WHERE 1=2");
                ResultSetMetaData resultSetMetaData = rsTable.getMetaData();
                //comentario
                ResultSet resultSetTable = databaseMetaData.getTables(null, null, table.getName(), new String[] { "TABLE", "VIEW" });
                if (resultSetTable != null) {
                    while (resultSetTable.next()) {
                        String comment = resultSetTable.getString(5);
                        table.setComment(comment);
                    }
                }
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    //fields
                    Column column = new Column();
                    column.setId(Util.generateId(255));
                    column.setName(resultSetMetaData.getColumnName(i));
                    column.setNullable(ResultSetMetaData.columnNullable == resultSetMetaData.isNullable(i));
                    column.setPk(pkContraint != null && pkContraint.getListField().contains(resultSetMetaData.getColumnName(i)));
                    column.setAutoIncrement(resultSetMetaData.isAutoIncrement(i));
                    column.setType(resultSetMetaData.getColumnTypeName(i));
                    column.setJavaType(resultSetMetaData.getColumnClassName(i));
                    column.setJavaSqlType(resultSetMetaData.getColumnType(i));
                    column.setUnique((pkContraint != null && pkContraint.getListField().contains(resultSetMetaData.getColumnName(i))) || (existUniqueColumn(listIndexContraint, resultSetMetaData.getColumnName(i))));
                    column.setPrecision(resultSetMetaData.getPrecision(i));
                    column.setScale(resultSetMetaData.getScale(i));
                    //comentario
                    ResultSet resultSetColumn = databaseMetaData.getColumns(null, null, table.getName(), column.getName());
                    if (resultSetColumn != null) {
                        while (resultSetColumn.next()) {
                            String comment = resultSetColumn.getString(12);
                            column.setComment(comment);
                            column.setDefaultValue(resultSetColumn.getString(13));
                        }
                    }
                    table.getColumn().add(column);
                }
                table.setPkContraint(pkContraint);
                table.getIndexContraint().addAll(listIndexContraint);
                //PROCESANDO LAS FOREING KEYS
                table.getFkContraint().addAll(tableFks(table));
                tableList.add(table);
                proccessed++;
            }
            progressInfo("Processed " + proccessed + " of " + total);
        } catch (SQLException e) {
            progressInfo(e.getMessage());
            throw new DataBaseReaderException(e.getMessage());
        }
        return tableList;
    }

    /**
     * existUniqueColumn
     * @param listUniqueInfo listUniqueInfo
     * @param columnName columnName
     * @return boolean
     */
    private boolean existUniqueColumn(List<IndexContraint> listUniqueInfo, String columnName) {
        boolean existe = false;
        if (listUniqueInfo != null) {
            for (IndexContraint uniqueContraint : listUniqueInfo) {
                if (uniqueContraint.getColumns().contains(columnName)) {
                    existe = true;
                    break;
                }
            }
        }
        return existe;
    }

    /**
     * readTablesName
     * @param customCon customCon
     * @param catalog catalog
     * @param schemaPattern schemaPattern
     * @param tableNamePattern tableNamePattern
     * @param types types
     * @param excludedTables excludedTables
     * @return List
     */
    protected List<Table> readTablesName(Connection customCon, String catalog, String schemaPattern, String tableNamePattern, String[] types, List<String> excludedTables) {
        List<Table> tables = new ArrayList<>();
        try {
            DatabaseMetaData databaseMetaData = customCon.getMetaData();
            ResultSet rs = (databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types));
            while (rs.next()) {
                String tableName = rs.getString(3);
                String tableType = rs.getString(4);
                if ((excludedTables == null || excludedTables.isEmpty()) || !excludedTables.contains(tableName.toUpperCase())) {
                    Table table = new Table();
                    table.setId(Util.generateId(255));
                    table.setName(tableName);
                    table.setType(tableType);
                    tables.add(table);
                }
            }
            rs.close();
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return tables;
    }

    /**
     * readBasicTableInfo
     * @param customCon customCon
     * @param catalog catalog
     * @param schemaPattern schemaPattern
     * @param tableNamePattern tableNamePattern
     * @param types types
     * @return List
     */
    protected List<Table> readBasicTableInfo(Connection customCon, String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        List<Table> tables = new ArrayList<>();
        if (excludedTables != null)
            excludedTables = excludedTables.stream().map(String::toLowerCase).collect(Collectors.toList());
        try {
            DatabaseMetaData databaseMetaData = customCon.getMetaData();
            ResultSet rs = (databaseMetaData.getTables(StringUtils.isBlank(catalog) ? null : catalog, StringUtils.isBlank(schemaPattern) ? null : schemaPattern, StringUtils.isBlank(tableNamePattern) ? null : tableNamePattern, types));
            Statement st = customCon.createStatement();
            ResultSet rsTable;
            ResultSetMetaData resultSetMetaData;
            List<String> pks;
            List<ImportedKey> fks;
            ImportedKey importedKey;
            while (rs.next()) {
                rsTable = null;
                String tableName = rs.getString(3);
                String tableType = rs.getString(4);
                if ((excludedTables == null || excludedTables.isEmpty()) || !excludedTables.contains(tableName.toLowerCase())) {
                    try {
                        rsTable = st.executeQuery("SELECT * FROM " + tableName + " WHERE 1=2");
                    } catch (Exception e) {
                        rsTable = null;
                        logger.error(e.getMessage());
                    }
                    if (rsTable != null) {
                        Table table = new Table();
                        table.setId(Util.generateId(255));
                        table.setName(tableName);
                        table.setType(tableType);
                        tables.add(table);
                        resultSetMetaData = rsTable.getMetaData();
                        pks = getTablePksContraints(customCon, catalog, schemaPattern, tableName);
                        fks = getSimpleTableFksColumInfo(customCon, catalog, schemaPattern, tableName);
                        table.setImportedKeys(fks);
                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                            Column column = new Column();
                            column.setId(Util.generateId(255));
                            column.setLabel(resultSetMetaData.getColumnLabel(i));
                            column.setName(resultSetMetaData.getColumnName(i));
                            column.setScale(resultSetMetaData.getScale(i));
                            column.setPrecision(resultSetMetaData.getPrecision(i));
                            column.setTable(table.getName());
                            column.setType(resultSetMetaData.getColumnTypeName(i));
                            column.setPk(pks.contains(column.getName()));
                            importedKey = fks.stream().filter(c -> (c.getFkColumnName().equalsIgnoreCase(column.getName()))).findFirst().orElse(null);
                            column.setFk(importedKey != null);
                            if (column.isFk() && importedKey != null)
                                column.setFktablename(importedKey.getPkTableName());
                            column.setAutoIncrement(resultSetMetaData.isAutoIncrement(i));
                            column.setJavaSqlType(resultSetMetaData.getColumnType(i));
                            column.setJavaType(resultSetMetaData.getColumnClassName(i));
                            column.setNullable(ResultSetMetaData.columnNullable == resultSetMetaData.isNullable(i));
                            table.getColumn().add(column);
                        }
                        if (pks != null && !pks.isEmpty()) {
                            table.setPkContraint(new PkContraint());
                            for (String columnName : pks) {
                                table.getPkContraint().getListField().add(columnName);
                            }
                        }
                    }
                }
            }
            rs.close();
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return tables;
    }

    /**
     * Table Pks name
     * @param con Connection
     * @param tableName table Name
     * @return PkContraint
     */
    protected PkContraint tablePks(Connection con, String tableName) {
        PkContraint pk = null;
        List<String> listField = new ArrayList<>();
        String pkName = null;
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            //ResultSet rsTablePK = databaseMetaData.getPrimaryKeys(database.getUser(), null, tableName);
            ResultSet rsTablePK = databaseMetaData.getPrimaryKeys(null, null, tableName);
            while (rsTablePK.next()) {
                listField.add(rsTablePK.getString(4));
                if (isEmpty(pkName)) {
                    pkName = rsTablePK.getString(6);
                }
            }
            rsTablePK.close();
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        if (!listField.isEmpty()) {
            pk = new PkContraint();
            pk.setName(pkName);
            pk.getListField().addAll(listField);
        }
        return pk;
    }

    /**
     * isEmpty
     * @param pkName pkName
     * @return boolean
     */
    private boolean isEmpty(String pkName) {
        return false;
    }

    /**
     * busca los indices de una tabla, esta informacion brinda
     * datos de indices de pk, indices de unique
     * @param con Connection
     * @param tableName table Name
     * @param dataBaseName data Base Name
     * @return List
     */
    protected List<IndexContraint> indexFields(Connection con, String tableName, String dataBaseName) {
        Map<String, IndexContraint> mapUnique = new HashMap<>();
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            ResultSet rs = databaseMetaData.getIndexInfo(dataBaseName, null, tableName, true, false);
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString(COLUMN_NAME);
                //1 es pk, //3 es unique
                String indexType = rs.getString("TYPE");
                if (isNotEmpty(indexName) && isNotEmpty(columnName)) {
                    IndexContraint uniqueContraint = mapUnique.get(indexName);
                    if (uniqueContraint == null) {
                        uniqueContraint = new IndexContraint();
                        uniqueContraint.setId(Util.generateId(255));
                        uniqueContraint.setName(indexName);
                        uniqueContraint.setType(indexType);
                        uniqueContraint.setOrderType("ASC_OR_DES");
                        mapUnique.put(indexName, uniqueContraint);
                    }
                    uniqueContraint.getColumns().add(columnName);
                }
            }
            rs.close();
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        List<IndexContraint> listUniqueContraint = new ArrayList<>();
        if (mapUnique.size() > 0) {
            listUniqueContraint.addAll(mapUnique.values());
        }
        return listUniqueContraint;
    }

    /**
     * isNotEmpty
     * @param columnName columnName
     * @return boolean
     */
    private boolean isNotEmpty(String columnName) {
        return (columnName != null && "".equals(columnName.trim()));
    }

    /**
     * getSimpleTableFksColumInfo
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param tableName tableName
     * @return List
     */
    protected List<ImportedKey> getSimpleTableFksColumInfo(Connection con, String catalog, String schema, String tableName) {
        List<ImportedKey> result = new ArrayList<>();
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            ResultSet foreignKeys = databaseMetaData.getImportedKeys(catalog, schema, tableName);
            ImportedKey importedKey;
            while (foreignKeys.next()) {
                importedKey = new ImportedKey();
                importedKey.setId(Util.generateId(255));
                importedKey.setPkTableName(foreignKeys.getString("PKTABLE_NAME"));
                importedKey.setFkColumnName(foreignKeys.getString("FKCOLUMN_NAME"));
                result.add(importedKey);
            }
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return result;
    }

    /**
     * getTableFksContraints
     * @param con con
     * @param tableName tableName
     * @return Collection
     */
    protected Collection<FkContraint> getTableFksContraints(Connection con, String tableName) {
        Map<String, FkContraint> mapFKs = new HashMap<>();
        String pkTableName;
        ImportedKey importedKey;
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, tableName);
            FkContraint fkContraint;
            while (foreignKeys.next()) {
                pkTableName = foreignKeys.getString("PKTABLE_NAME");
                if (!mapFKs.containsKey(pkTableName)) {
                    fkContraint = new FkContraint();
                    fkContraint.setId(Util.generateId(255));
                    fkContraint.setPkTableName(pkTableName);
                    fkContraint.setFkName(foreignKeys.getString("FK_NAME"));
                    fkContraint.setFkTableName(tableName);
                    mapFKs.put(pkTableName, fkContraint);
                }
                importedKey = new ImportedKey();
                importedKey.setId(Util.generateId(255));
                importedKey.setPkTableName(foreignKeys.getString("PKTABLE_NAME"));
                importedKey.setDeleteRule(foreignKeys.getString("DELETE_RULE"));
                importedKey.setFkColumnName(foreignKeys.getString("FKCOLUMN_NAME"));
                importedKey.setKeySeq(foreignKeys.getString("KEY_SEQ"));
                importedKey.setPkColumnName(foreignKeys.getString("PKCOLUMN_NAME"));
                mapFKs.get(pkTableName).getImportedKey().add(importedKey);
            }
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return new ArrayList<>(mapFKs.values());
    }

    /**
     * Devuelve todas las contraint por fk de una tabla
     * @param table table
     * @return List
     * @throws java.sql.SQLException SQLException
     */
    protected List<FkContraint> tableFks(Table table) throws SQLException {
        List<FkContraint> relationList = new ArrayList<>();
        DatabaseMetaData databaseMetaData = con.getMetaData();
        String tableName = table.getName();
        //progress information
        progressInfo("Last process: Updateting Relation... Loading imported keys for table:" + tableName);
        //Primero se agrupan todas las foreing keys asociadas a la tabla.
        //Recordar que una ForeingKey es una estructura que informa los campos
        //de la tabla que son fk y con que otra tabla y campo(pk) tiene la asociacion.
        //<nombre contraint,objeto fk contraint>
        Map<String, FkContraint> mapFkContraint = new HashMap<>();
        //el Record set no trae las foreing keys agrupadas,
        //hay que hacer el trabajo manualmente
        String fkName;
        //informacion sobre las llaves foraneas de la tabla tableName
        ResultSet rs = databaseMetaData.getImportedKeys(null, null, tableName);
        //por cada informacion de foreing key de la tabla que se esta
        //procesando en estos momentos ->tableName
        while (rs.next()) {
            fkName = rs.getString(FK_NAME);
            FkContraint fkContraint = mapFkContraint.get(fkName);
            //si null nunca ha sido procesada
            if (fkContraint == null) {
                fkContraint = new FkContraint();
                fkContraint.setId(Util.generateId(255));
                //nombre la contraint, relacion, o foreing key
                fkContraint.setFkName(fkName);
                fkContraint.setFkTableName(tableName);
                //tabla con la cual se tiene la relacion
                fkContraint.setPkTableName(rs.getString(PKTABLE_NAME));
                //foreingKey.setPkName(rs.getString(PK_NAME));//contraing de la llave primaria de la tabla con la cual se tiene relacion
                mapFkContraint.put(fkName, fkContraint);
            }
            //esta ya es la informacion de un campo involucrado en la contrain
            ImportedKey importedKey = new ImportedKey();
            importedKey.setId(Util.generateId(255));
            importedKey.setPkColumnName(rs.getString(PKCOLUMN_NAME));
            importedKey.setFkColumnName(rs.getString(FKCOLUMN_NAME));
            importedKey.setUpdateRule(rs.getString(UPDATE_RULE));
            importedKey.setDeleteRule(rs.getString(DELETE_RULE));
            mapFkContraint.get(fkName).getImportedKey().add(importedKey);
            //marcar como foreing key al campo de la tabla
            //field.name es el nombre del campo en la tabla
            //Esta informacion es muy importante porque cuando
            //se realiza ing inversa los campos que fk no van
            //como atributos de la clase, sino que en la relacion
            //se informa el nombre del campo
            for (Column column : table.getColumn()) {
                if (column.getName().equals(importedKey.getFkColumnName())) {
                    column.setFk(true);
                    importedKey.setNullable(column.isNullable());
                    importedKey.setUnique(column.isNullable());
                    importedKey.setPk(column.isPk());
                    //importedKey.setAutoIncrement(field.isAutoIncrement());
                    break;
                }
            }
        }
        if (mapFkContraint.size() > 0) {
            relationList.addAll(mapFkContraint.values());
        }
        return relationList;
    }

    /**
     * progressInfo
     * @param msg msg
     */
    public void progressInfo(String msg) {
        log = msg;
    }

    /**
     * process Imported Exported Keys
     * @param tableName table Name
     * @param rs ResultSet
     * @return Map
     * @throws java.lang.Exception Exception
     */
    protected Map<String, List<Properties>> processImported_Exported_Keys(String tableName, ResultSet rs) throws Exception {
        Map<String, List<Properties>> assocMap = new HashMap<>();
        String currentAssocTable = "";
        List<Properties> propList = null;
        while (rs.next()) {
            Properties p = new Properties();
            String fkTableName = rs.getString(tableName);
            //check if it's first record or the table information change
            if ("".equals(currentAssocTable) || !currentAssocTable.equals(fkTableName)) {
                if (!"".equals(currentAssocTable)) {
                    //store the last association information
                    assocMap.put(currentAssocTable, propList);
                }
                //preparte values to the new association table
                currentAssocTable = fkTableName;
                propList = new ArrayList<>();
            }
            //table primary key
            p.setProperty(PKCOLUMN_NAME, rs.getString(PKCOLUMN_NAME));
            //association table key or foregin key
            p.setProperty(FKCOLUMN_NAME, rs.getString(FKCOLUMN_NAME));
            propList.add(p);
        }
        //the last iformation never set,  do it here
        if (!"".equals(currentAssocTable)) {
            assocMap.put(currentAssocTable, propList);
        }
        return assocMap;
    }

    /**
     * Description: Read and process all tables(with their pk fields) that used
     * pk fields from tableName
     * @param con Connection
     * @param tableName table Name
     * @return Map
     */
    protected Map<String, List<Properties>> getExportedKeys(Connection con, String tableName) {
        Map<String, List<Properties>> result = null;
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            //ResultSet rs = databaseMetaData.getExportedKeys(database.getUser(), null, tableName);
            ResultSet rs = databaseMetaData.getExportedKeys(null, null, tableName);
            result = processImported_Exported_Keys(FKTABLE_NAME, rs);
            rs.close();
        } catch (Exception ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return result;
    }

    /**
     * getLog
     * @return String
     */
    public String getLog() {
        return log;
    }

    /**
     * getMetadata
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @return List
     */
    @Override
    public List<Table> getMetadata(Connection con, String catalog, String schema) {
        return getMetadata(con, catalog, schema, null);
    }

    /**
     * getMetadata
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param table table
     * @return List
     */
    public List<Table> getMetadata(Connection con, String catalog, String schema, String table) {
        return readBasicTableInfo(con, catalog, schema, table, new String[] { "TABLE" });
    }

    /**
     * getTableColumnMetaData
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param tableName tableName
     * @return List
     */
    @Override
    public List<Column> getTableColumnMetaData(Connection con, String catalog, String schema, String tableName) {
        List<Column> columns = new ArrayList<>();
        try {
            Statement st = con.createStatement();
            ResultSet rsTable = st.executeQuery("SELECT * FROM " + tableName + " WHERE 1=2");
            ResultSetMetaData resultSetMetaData = rsTable.getMetaData();
            List<String> pks = getTablePksContraints(con, catalog, schema, tableName);
            List<ImportedKey> fks = getSimpleTableFksColumInfo(con, catalog, schema, tableName);
            ImportedKey importedKey;
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                Column column = new Column();
                column.setId(Util.generateId(255));
                column.setName(resultSetMetaData.getColumnName(i));
                column.setAutoIncrement(resultSetMetaData.isAutoIncrement(i));
                column.setType(resultSetMetaData.getColumnTypeName(i));
                column.setJavaSqlType(resultSetMetaData.getColumnType(i));
                column.setJavaType(resultSetMetaData.getColumnClassName(i));
                column.setNullable(ResultSetMetaData.columnNullable == resultSetMetaData.isNullable(i));
                column.setPrecision(resultSetMetaData.getPrecision(i));
                column.setScale(resultSetMetaData.getScale(i));
                column.setPk(pks.contains(column.getName()));
                importedKey = fks.stream().filter((c) -> (c.getFkColumnName().equalsIgnoreCase(column.getName()))).findFirst().orElse(null);
                column.setFk(importedKey != null);
                if (column.isFk())
                    column.setFktablename(importedKey.getPkTableName());
                columns.add(column);
            }
        } catch (SQLException ex) {
            progressInfo(ex.getMessage());
            logger.error(ex.getMessage());
        }
        return columns;
    }

    /**
     * getTablePksContraints
     * @param con con
     * @param catalog catalog
     * @param schema schema
     * @param table table
     * @return List
     */
    @Override
    public List<String> getTablePksContraints(Connection con, String catalog, String schema, String table) {
        List<String> list = new ArrayList<>();
        try {
            ResultSet rs = con.getMetaData().getPrimaryKeys(catalog, schema, table);
            while (rs.next()) {
                list.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * close
     * @throws SQLException SQLException
     */
    public void close() throws SQLException {
        if (con != null)
            con.close();
    }

    /**
     * schemaExists
     * @param schema schema
     * @return boolean
     * @throws SQLException SQLException
     */
    public boolean schemaExists(String schema) throws SQLException {
        boolean exists = false;
        try (ResultSet resultSet = con.getMetaData().getCatalogs()) {
            while (resultSet.next()) {
                // Get the database name, which is at position 1
                if (schema.equalsIgnoreCase(resultSet.getString(1))) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }
}
