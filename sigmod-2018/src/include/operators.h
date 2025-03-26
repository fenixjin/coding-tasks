#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <set>
#include <mutex>
#include <cassert>


#include "relation.h"
#include "parser.h"
#include "column.hpp"

namespace std {
/// Simple hash function to enable use with unordered_map
template<>
struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const &s) const noexcept {
        return s.binding ^ (s.col_id << 5);
    }
};
};

/// Operators materialize their entire result
class Operator {
protected:
    /// Mapping from select info to data
    std::unordered_map<SelectInfo, unsigned> select_to_result_col_id_;
    /// The tmp results
    std::vector<std::vector<std::vector<uint64_t>>> tmp_results_;
    /// The result size
    uint64_t result_size_ = 0;
    /// The materialized results
    std::vector<Column<uint64_t>> results;

    // parent Operator, this is for running parent operator when child operator has finished
    std::weak_ptr<Operator> parent;

    // if 0, all asyncrunning input operator has finished.
    int pendingAsyncOperator =-1;


    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) { throw; }

    virtual void printAsyncInfo() = 0;

    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false); 


public:
    /// The destructor
    virtual ~Operator() = default;;

    bool is_stopped = false;
    uint64_t resultSize=0;

    /// Require a column and add it to results
    virtual bool require(SelectInfo info) = 0;
    /// Resolves a column
    unsigned resolve(SelectInfo info) {
        assert(select_to_result_col_id_.find(info) != select_to_result_col_id_.end());
        return select_to_result_col_id_[info];
    }
    /// Run
    virtual void run() = 0;

    uint64_t result_size() const {
        return result_size_;
    }

    /// Get  materialized results
    virtual std::vector<Column<uint64_t>>& getResults();
    /// Get materialized results size in bytes
    virtual uint64_t getResultsSize();

    void setParent(std::shared_ptr<Operator> parent) { this->parent = parent; }

    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) = 0;

    virtual void stop() {is_stopped = true; __sync_synchronize();}

};

class Scan : public Operator {
protected:
    /// The relation
    Relation &relation_;
    /// The name of the relation in the query
    unsigned relation_binding_;
    // required info
    std::vector<SelectInfo> infos;

public:
    /// The constructor
    Scan(Relation &r, unsigned relation_binding)
        : relation_(r), relation_binding_(relation_binding) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    virtual uint64_t getResultsSize() override;
    // Print async info
	virtual void printAsyncInfo() override;
    std::vector<Column<uint64_t>>& getResults();
};

class FilterScan : public Scan {
private:
    /// The filter info
    std::vector<FilterInfo> filters_;
    /// The input data
    std::vector<uint64_t *> input_data_;
    std::vector<std::vector<std::vector<uint64_t>>> tmpResults; // [partition][col][tuple]
    bool applyFilter(uint64_t id,FilterInfo& f);
    void copy2Result(uint64_t id);
    int pendingTask = -1;
    
    unsigned minTuplesPerTask = 1000;

    void filterTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);

public:
    /// The constructor
    FilterScan(const Relation &r, std::vector<FilterInfo> filters)
        : Scan(r,
               filters[0].filter_column.binding),
          filters_(filters) {};
    /// The constructor
    FilterScan(const Relation &r, FilterInfo &filter_info)
        : FilterScan(r,
                     std::vector<
                     FilterInfo> {
        filter_info
    }) {};

    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// Run
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;

    virtual uint64_t getResultsSize() override { return Operator::getResultsSize(); }
};

class Join : public Operator {
private:
    /// The input operators
    std::shared_ptr<Operator> left_, right_;
    /// The join predicate info
    PredicateInfo p_info_;

    using HT = std::unordered_multimap<uint64_t, uint64_t>;

    /// The hash table for the join
    HT hash_table_;
    /// Columns that have to be materialized
    std::unordered_set<SelectInfo> requested_columns_;
    /// Left/right columns that have been requested
    std::vector<SelectInfo> requested_columns_left_, requested_columns_right_;

    int pending_task = -1;
    uint64_t minTuplesPerTask = 1000;
    /// The entire input data of left and right
    std::vector<Column<uint64_t>> left_input_data_, right_input_data_;
    /// The input data that has to be copied
    std::vector<Column<uint64_t>*> copy_left_data_, copy_right_data_;

    uint64_t task_length;
    uint64_t task_remainder;

private:
    /// Copy tuple to result
    void copy2Result(uint64_t left_id, uint64_t right_id);
    /// Create mapping for bindings
    void createMappingForBindings();

    void probingTask(boost::asio::io_service* ioService, int partIndex, int taskIndex, uint64_t right_col_id, uint64_t start, uint64_t length);

public:
    /// The constructor
    Join(std::shared_ptr<Operator> &left,
         std::shared_ptr<Operator> &right,
         const PredicateInfo &p_info)
        : left_(left), right_(right), p_info_(p_info) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
	// Print async info
	virtual void printAsyncInfo() override;
	/// stop all children 
    virtual void stop() {
        is_stopped = true;
        __sync_synchronize();
        if (!left_->is_stopped)
            left_->stop();
        if (!right_->is_stopped)
            right_->stop(); 
    }
};

class SelfJoin : public Operator {
private:
    /// The input operators
    std::shared_ptr<Operator> input_;
    /// The join predicate info
    PredicateInfo p_info_;
    /// The required IUs
    std::set<SelectInfo> required_IUs_;

    /// tmpResults
	std::vector<std::vector<std::vector<uint64_t>>> tmpResults;
    
    /// The entire input data
    std::vector<Column<uint64_t>> input_data_;
    /// The input data that has to be copied
    std::vector<Column<uint64_t>*> copy_data_;

    int pendingTask = -1;
    unsigned minTuplesPerTask = 1000;

    void selfJoinTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);


private:
    /// Copy tuple to result
    void copy2Result(uint64_t id);

public:
    /// The constructor
    SelfJoin(std::shared_ptr<Operator> &input, PredicateInfo &p_info)
        : input_(std::move(input)), p_info_(p_info) {};
    /// Require a column and add it to results
    bool require(SelectInfo info) override;
    /// AsyncRun
    virtual void asyncRun(boost::asio::io_service& ioService) override;
    /// only call it if pendingAsyncOperator=0, and can getResults()
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
	// Print async info
	virtual void printAsyncInfo() override;
};

class Checksum : public Operator {
private:

    Joiner& joiner;
    /// The input operator
    std::shared_ptr<Operator> input_;
    /// The join predicate info
    const std::vector<SelectInfo> col_info_;

    std::vector<uint64_t> check_sums_;
    int query_index;
    int pending_task = -1;
    unsigned min_tuples_per_task = 1000;
    void checksumTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length);
    

public:
    /// The constructor
    Checksum(Joiner& joiner, std::shared_ptr<Operator> &input,
             std::vector<SelectInfo> col_info)
        : joiner(joiner), input_(input), col_info_(col_info) {};
    /// Request a column and add it to results
    bool require(SelectInfo info) override {
        // check sum is always on the highest level
        // and thus should never request anything
        throw;
    }
    virtual void asyncRun(boost::asio::io_service& ioService, int queryIndex);
    virtual void asyncRun(boost::asio::io_service& ioService) {}
    virtual void createAsyncTasks(boost::asio::io_service& ioService) override;
    virtual void finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync=false) override;
	virtual void printAsyncInfo() override;

    /// Run
    void run() override;

    const std::vector<uint64_t> &check_sums() {
        return check_sums_;
    }
};

