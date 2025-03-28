#pragma once

#include <vector>
#include <cstdint>
#include <set>
#include <condition_variable>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>
#include <thread>

#include "operators.hpp"
#include "relation.hpp"
#include "parser.hpp"
#include "config.hpp"
#include "utils.hpp"

class Checksum;
class Operator;

class Joiner {
    friend Checksum;
private:
    /// The relations that might be joined
    std::vector<Relation> relations_;
    boost::asio::io_service ioService;
    boost::thread_group threadPool;
    boost::asio::io_service::work work;

    int pendingAsyncJoin = 0;
    int nextQueryIndex = 0;
    std::vector<std::vector<uint64_t>> asyncResults; //checksums
    std::vector<std::shared_ptr<Checksum>> asyncJoins;

    std::condition_variable cvAsync;
    std::mutex cvAsyncMutex;
    char* buf[THREAD_NUM];
    int cntTouch; 

public:

    /// Add relation
    void addRelation(const char *file_name);
    void addRelation(Relation &&relation);
    /// Get relation
    Relation &getRelation(unsigned relation_id);
    /// Joins a given set of relations
    void join(QueryInfo& i, int queryIndex);

    const std::vector<Relation> &relations() const {
        return relations_;
    }

    void waitAsyncJoins();

    std::vector<std::string> getAsyncJoinResults();

    void createAsyncQueryTask(std::string line);

    Joiner(int threadNum) : work(ioService) {
        asyncJoins.reserve(100);
        asyncResults.reserve(100);

        for(int i = 0; i < threadNum; i++) {
            threadPool.create_thread([&]() {
                tid = __sync_fetch_and_add(&nextTid, 1);
                ioService.run();
            });
        }
    }
    ~Joiner() {
        ioService.stop();
    }

private:
    /// Add scan to query
    std::shared_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                      const SelectInfo &info,
                                      QueryInfo &query);


};

