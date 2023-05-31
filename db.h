#ifndef DB_H
#define DB_H

#include <string>
#include "mysql_connection.h"
#include "mysql_driver.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <nlohmann/json.hpp>
#include <list>
#include <map>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <mutex>
#include <exception>
#include <string>


class Database {

	public:
		sql::mysql::MySQL_Driver* driver;
		sql::Connection* con;
		std::string connUUID;

		Database(std::string connUUID, std::string host, std::string username, std::string password, std::string database);
		bool isConnected();
		nlohmann::json extractRows(sql::ResultSet* result);
		nlohmann::json extractSingleRow(sql::ResultSet* result);
};

class Manager {
	public:
		Manager(int minPoolSize, int maxPoolSize, std::string host, std::string username, std::string password, std::string database);
		Database* getConnection();
		void releaseConnection(Database* connection);

	private:
		std::list<Database*> pool;
                std::map<std::string, Database*> checkoutConnections;
                boost::uuids::random_generator generator;
		std::mutex mutex;
		std::string host, username, password, database;
		int activeConnections = 0;
		int maxPoolSize;

		void initPool(int minPoolSize);
		Database* CreateFreshConnection();
};


class PoolOverFlowException : public std::exception {
	public:
		PoolOverFlowException(int maxPoolSize) {
			std::ostringstream oss;
			oss << "The connection pool of size " << maxPoolSize << " is overflowing. No connections available at the moment.";
			message_ = oss.str();
		}

		const char* what() const noexcept override {
			return message_.c_str();
		}
	
	private:
		std::string message_;
};


#endif
