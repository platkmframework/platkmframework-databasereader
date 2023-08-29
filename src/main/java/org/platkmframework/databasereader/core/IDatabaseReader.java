package org.platkmframework.databasereader.core;

import java.sql.Connection;
import java.util.List;

import org.platkmframework.databasereader.model.Table;

public interface IDatabaseReader {
	
	List<Table> getMetadata(Connection con);

}
