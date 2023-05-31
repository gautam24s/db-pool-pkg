#include "db.h"
#include "mysql_connection.h"
#include "mysql_driver.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <nlohmann/json.hpp>
#include <boost/uuid/uuid.hpp>
#include <list>
#include <map>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <mutex>


	
Database::Database(std::string connUUIDStr, std::string host, std::string username, std::string password, std::string database) 
{
	driver =  sql::mysql::get_driver_instance();
	con = driver->connect(host, username, password);
	con->setSchema(database);
	connUUID = connUUIDStr;
}

bool Database::isConnected()
{
	if (con->isValid()) {
		return true;
	} else {
		return false;
	}
}

nlohmann::json	Database::extractRows(sql::ResultSet *result)
{
	nlohmann::json jsonResult;
	sql::ResultSetMetaData* meta = result->getMetaData();
	int columnCount = meta->getColumnCount();
	
	while (result->next()) {
		nlohmann::json row;
		for (int i = 1; i <= columnCount; i++) {
			auto columnName = meta->getColumnName(i);
			sql::SQLString columnType = meta->getColumnTypeName(i);
			std::string columnTypeStr = columnType.asStdString();
			if(columnTypeStr == "INT") {
				int columnValue = result->getInt(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DOUBLE") {
				double columnValue = result->getDouble(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "VARCHAR") {
				std::string columnValue = result->getString(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DATE") {
				std::string columnValue = result->getString(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "BIGINT") {
				std::string columnValueStr = result->getString(i);
				std::int64_t columnValue = std::stoll(columnValueStr); 
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DECIMAL") {
				std::string columnValueStr = result->getString(i);
				double columnValue = std::stod(columnValueStr);
                                row[columnName] = columnValue;
			}
		}
		jsonResult.push_back(row);
	}
	return jsonResult;
}

nlohmann::json  Database::extractSingleRow(sql::ResultSet *result) 
{
	nlohmann::json row;
	sql::ResultSetMetaData* meta = result->getMetaData();
	int columnCount = meta->getColumnCount();
	if (result->next()) {
		for (int i = 1; i <= columnCount; i++) {
			auto columnName = meta->getColumnName(i);
			sql::SQLString columnType = meta->getColumnTypeName(i);
			std::string columnTypeStr = columnType.asStdString();
			if(columnTypeStr == "INT") {
				int columnValue = result->getInt(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DOUBLE") {
				double columnValue = result->getDouble(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "VARCHAR") {
				std::string columnValue = result->getString(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DATE") {
				std::string columnValue = result->getString(i);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "BIGINT") {
				std::string columnValueStr = result->getString(i);
				std::int64_t columnValue = std::stoll(columnValueStr);
				row[columnName] = columnValue;
			} else if (columnTypeStr == "DECIMAL") {
				std::string columnValueStr = result->getString(i);
				double columnValue = std::stod(columnValueStr);
				row[columnName] = columnValue;
			}
		}
	}
	return row;
}


Database* Manager::CreateFreshConnection() {
	boost::uuids::uuid connUUID = generator();
	std::string connUUIDStr = boost::uuids::to_string(connUUID);
	return new Database(connUUIDStr, host, username, password, database);
}


Database* Manager::getConnection() {
	std::unique_lock<std::mutex> lock(mutex);
	Database* conn;

	if (pool.size() == 0 && activeConnections >= maxPoolSize) {
		throw PoolOverFlowException(maxPoolSize);
	}	

	// check if connection available in pool or not
	// pull if connection available
	// check if connection is still conneccted to db
	// if not able to get connection from pool, then create new connection
	if (pool.size() > 0) {
		conn = pool.front();
		pool.pop_front();
		if (!conn->isConnected()) {
			activeConnections -= 1;
			lock.unlock();
			return getConnection();
		}
	} else {
		conn = CreateFreshConnection();
		activeConnections += 1;
	}
	checkoutConnections[conn->connUUID] = conn;
	return conn;
}

void Manager::releaseConnection(Database* conn) {
	std::unique_lock<std::mutex> lock(mutex);
	checkoutConnections.erase(conn->connUUID);
	pool.push_back(conn);
}

void Manager::initPool(int minPoolSize) {
	for (int i=1; i<=minPoolSize; i++) {
		pool.push_back(CreateFreshConnection());
		activeConnections += 1;
	}
}

Manager::Manager(int minPoolSize, int maxPoolSize, std::string hostName, std::string userName, std::string passwordStr, std::string databaseName) :maxPoolSize(maxPoolSize) {
	host = hostName;
	username = userName;
	password = passwordStr;
	database = databaseName;
	initPool(minPoolSize);
}
