#include "operators.hpp"
#include "joiner.hpp"
#include <cassert>

// Get materialized results
std::vector<Column<uint64_t>>& Operator::getResults() {
    return results;
}

void Operator::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    is_stopped = true;
    if (auto p = parent.lock()) {
        if (result_size() == 0)
            p->stop();
        int pending = __sync_sub_and_fetch(&p->pendingAsyncOperator, 1);
        assert(pending>=0);
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun parent's pending: " << pending << endl;
#endif
        if (pending == 0 && startParentAsync) 
            p->createAsyncTasks(ioService);
    } else {
#ifdef VERBOSE
    cout << "Operator("<< queryIndex << "," << operatorIndex <<")::finishAsyncRun has no parent" << endl;
#endif
        // root node
    }
}

uint64_t Operator::getResultsSize() {
    return result_size_ * results.size() * 8;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info)
{
    if (info.binding != relation_binding_)  {
        return false;
    }
    assert(info.col_id < relation_.columns_.size());
    if (select_to_result_col_id_.find(info)==select_to_result_col_id_.end()) {
//        resultColumns.push_back(relation.columns[info.colId]);
        results.emplace_back(1);
        infos.push_back(info);
		select_to_result_col_id_[info]=results.size()-1;
    }
    return true;
}

void Scan::asyncRun(boost::asio::io_service& ioService) {
    #ifdef VERBOSE
        cout << "Scan("<< queryIndex << "," << operatorIndex <<")::asyncRun, Task" << endl;
    #endif
        pendingAsyncOperator = 0;
        for (int i = 0; i < infos.size(); i++) {
            results[i].addTuples(0, relation_.columns_[infos[i].col_id], relation_.size());
            results[i].fix();
        }
        result_size_=relation_.size(); 
        
        finishAsyncRun(ioService, true);
    }

// Get materialized results
std::vector<Column<uint64_t>>& Scan::getResults() {
    return results;
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info) {
    if (info.binding != relation_binding_)
        return false;

    assert(info.col_id<relation_.columns_.size());
    unsigned col_ID = 0;
    if (select_to_result_col_id_.find(info) == select_to_result_col_id_.end()) {
        // Add to results
        select_to_result_col_id_[info] = infos.size();
        infos.push_back(info);
    }
    return true;
}

bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
// Apply filter
{
    auto compareCol = relation_.columns_[f.filter_column.col_id];
    auto constant=f.constant;
    switch (f.comparison) {
        case FilterInfo::Comparison::Equal:
            return compareCol[i]==constant;
        case FilterInfo::Comparison::Greater:
            return compareCol[i]>constant;
        case FilterInfo::Comparison::Less:
            return compareCol[i]<constant;
    };
    return false;
}

void FilterScan::asyncRun(boost::asio::io_service& ioService) {
    #ifdef VERBOSE
        cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
    #endif
        pendingAsyncOperator = 0;
        __sync_synchronize();
        createAsyncTasks(ioService);  
}

void FilterScan::createAsyncTasks(boost::asio::io_service& ioService) {
    #ifdef VERBOSE
        cout << "FilterScan("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
    #endif  
    //const uint64_t partitionSize = L2_SIZE/2;
    //const unsigned taskNum = CNT_PARTITIONS(relation.size*relation.columns.size()*8, partitionSize);
    __sync_synchronize();
    if (is_stopped) {
#ifdef ANALYZE_STOP
        __sync_fetch_and_add(&fsStopCnt, 1);
#endif
//            cerr << "stop" << endl;
        finishAsyncRun(ioService, true);
        return;
    }
    int cnt_task = THREAD_NUM;
    uint64_t size = relation_.size();
    if (infos.size() == 1){
        bool pass = true;
        // 不知道为什么这里要检查filter 和 selectinfo是不是对的上
        for (auto &f : filters_){
            if (f.filter_column.col_id != infos[0].col_id){
                pass = false;
                break;
            }
        }
    }

    uint64_t taskLength = size/cnt_task;
    uint64_t rest = size%cnt_task;
    
    if (taskLength < minTuplesPerTask) {
        cnt_task = size/minTuplesPerTask;
        if (cnt_task == 0)
        cnt_task = 1;
        taskLength = size/cnt_task;
        rest = size%cnt_task;
    }
    
    pendingTask = cnt_task;
    
    for (auto &sInfo : infos) {
        input_data_.emplace_back(relation_.columns_[sInfo.col_id]);
        results.emplace_back(cnt_task);
    }
    
    for (int i=0; i<cnt_task; i++) {
        tmpResults.emplace_back();
    }
    
    __sync_synchronize(); 
    // uint64_t length = partitionSize/(relation.columns.size()*8); 
    uint64_t start = 0;
    for (unsigned i=0; i<cnt_task; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&FilterScan::filterTask, this, &ioService, i, start, length)); 
        start += length;
    }
}

void FilterScan::filterTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    unsigned colSize = input_data_.size();
    unordered_map<uint64_t, unsigned> cntMap;
    
    __sync_synchronize();
    if (is_stopped) {
#ifdef ANALYZE_STOP
        __sync_fetch_and_add(&fsTaskStopCnt, 1);
#endif 
//            cerr << "stop" << endl;
        goto fs_finish;
    }
    
    for (int j=0; j<input_data_.size(); j++) {
        localResults.emplace_back();
    }

    for (uint64_t i=start;i<start+length;++i) {
        bool pass=true;
        for (auto& f : filters_) {
            if(!(pass=applyFilter(i,f)))
                break;
            auto compareCol = relation_.columns_[f.filter_column.col_id];
            // if (pass && compareCol[i] > 783) {
            //     std::cout << compareCol[i] << " passed" << endl;
            // } 
        }
        if (pass) {
            
            // If count == 2, colSize already contains count column
            for (unsigned cId=0;cId<colSize;++cId)
                localResults[cId].push_back(input_data_[cId][i]);
        }
    }

    for (unsigned cId=0;cId<colSize;++cId) {
		results[cId].addTuples(taskIndex, localResults[cId].data(), localResults[cId].size());
    }

    //resultSize += localResults[0].size();
	__sync_fetch_and_add(&result_size_, localResults[0].size());

fs_finish:
    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (remainder == 0) {
        for (unsigned cId=0;cId<colSize;++cId) {
            results[cId].fix();
        }
        finishAsyncRun(*ioService, true);
    }
}

// // Copy to result
// void FilterScan::copy2Result(uint64_t id) {
//     for (unsigned cId = 0; cId < input_data_.size(); ++cId)
//         tmp_results_[cId].push_back(input_data_[cId][id]);
//     ++result_size_;
// }

// Require a column and add it to results
bool Join::require(SelectInfo info) {
    if (requested_columns_.count(info) == 0) {
        bool success = false;
        if (left_->require(info)) {
            requested_columns_left_.emplace_back(info);
            success = true;
        } else if (right_->require(info)) {
            success = true;
            requested_columns_right_.emplace_back(info);
        }
        if (!success)
            return false;

        requested_columns_.emplace(info);
    }
    return true;
}

// Copy to result
// void Join::copy2Result(uint64_t left_id, uint64_t right_id) {
//     // unsigned rel_col_id = 0;
//     // for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
//     //     tmp_results_[rel_col_id++].push_back(copy_left_data_[cId][left_id]);

//     // for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
//     //     tmp_results_[rel_col_id++].push_back(copy_right_data_[cId][right_id]);
//     for (int i = 0; i < )
//     ++result_size_;
// }

// // Run
// void Join::run() {
//     left_->require(p_info_.left);
//     right_->require(p_info_.right);
//     left_->run();
//     right_->run();

//     // Use smaller input_ for build
//     if (left_->result_size() > right_->result_size()) {
//         std::swap(left_, right_);
//         std::swap(p_info_.left, p_info_.right);
//         std::swap(requested_columns_left_, requested_columns_right_);
//     }

//     auto left_input_data = left_->getResults();
//     auto right_input_data = right_->getResults();

//     // Resolve the input_ columns_
//     unsigned res_col_id = 0;
//     for (auto &info : requested_columns_left_) {
//         copy_left_data_.push_back(left_input_data[left_->resolve(info)]);
//         select_to_result_col_id_[info] = res_col_id++;
//     }
//     for (auto &info : requested_columns_right_) {
//         copy_right_data_.push_back(right_input_data[right_->resolve(info)]);
//         select_to_result_col_id_[info] = res_col_id++;
//     }

//     auto left_col_id = left_->resolve(p_info_.left);
//     auto right_col_id = right_->resolve(p_info_.right);

//     // Build phase
//     auto left_key_column = left_input_data[left_col_id];
//     hash_table_.reserve(left_->result_size() * 2);
//     auto left_key_iter = left_key_column.begin(0);
//     for (uint64_t i = 0, limit = i + left_->result_size(); i != limit; i++, ++left_key_iter) {
//         hash_table_.emplace(*left_key_iter, i);
//     }   
//     // Probe phase
//     auto right_key_column = right_input_data[right_col_id];
//     auto r_key_col_iter = right_key_column.begin(0);
//     for (uint64_t i = 0, limit = i + right_->result_size(); i != limit; ++i) {
//         auto rightKey = *r_key_col_iter;
//         auto range = hash_table_.equal_range(rightKey);
//         for (auto iter = range.first; iter != range.second; ++iter) {
//             for(int j = 0; j < left_input_data_.size(); j++){
//                 results[j].push_back(iter);
//             }
//         }
//     }
// }
void Join::asyncRun(boost::asio::io_service& ioService) {
    #ifdef VERBOSE
        cout << "Join("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
    #endif
        pendingAsyncOperator = 2;
        left_->require(p_info_.left);
        right_->require(p_info_.right);
        __sync_synchronize();
        left_->asyncRun(ioService);
        right_->asyncRun(ioService);
}

void Join::createAsyncTasks(boost::asio::io_service& ioService) {
    // currently there is no multithread for join
    // all steps of join will run in this function
    assert (pendingAsyncOperator==0);
    
    __sync_synchronize();
    if (is_stopped) {
#ifdef ANALYZE_STOP
        __sync_fetch_and_add(&joinStopCnt, 1);
#endif
//            cerr << "stop" << endl;
        finishAsyncRun(ioService, true);
        return;
    }
    // Use smaller input_ for build
    if (left_->result_size() > right_->result_size()) {
        swap(left_,right_);
        swap(p_info_.left,p_info_.right);
        swap(requested_columns_left_,requested_columns_right_);
    }

    auto& left_input_data = left_->getResults();
    auto& right_input_data = right_->getResults();
    vector<Column<uint64_t>> copyLeftData, copyRightData;
    // Resolve the input_ columns_
    // 在这里join会建立自己的, 但是requested_columns_left_只是query select中请求的部分，
    // 不是join predicate 中请求的部分
    unsigned res_col_id = 0;
    for (auto &info : requested_columns_left_) {
        select_to_result_col_id_[info] = res_col_id++;
        copyLeftData.emplace_back(left_input_data[left_->resolve(info)]);
    }
    for (auto &info : requested_columns_right_) {
        select_to_result_col_id_[info] = res_col_id++;
        copyRightData.emplace_back(right_input_data[right_->resolve(info)]);
    }

    if (left_->result_size() == 0) { // result is empty
        finishAsyncRun(ioService, true);
        return;
    }

    unsigned col_size = requested_columns_left_.size() 
        + requested_columns_right_.size();
    for(unsigned i = 0; i < col_size; i++) {
        tmp_results.emplace_back();
        results.emplace_back(1);
    }
    
    vector<Column<uint64_t>::Iterator> r_col_iters;
    // for(int i = 0; i < left_input_data.size(); i++) {
    //     col_iters.emplace_back(left_input_data[i].begin(0));
    // }
    for(int i = 0; i < requested_columns_right_.size(); i++) {
        r_col_iters.emplace_back(copyRightData[i].begin(0));
    }

    auto left_col_id = left_->resolve(p_info_.left);
    auto right_col_id = right_->resolve(p_info_.right);
    // Build phase
    auto l_key_col_iter = left_input_data[left_col_id].begin(0);
    hash_table_.reserve(left_->result_size() * 2);
    for (uint64_t i = 0, limit = i + left_->result_size(); i != limit; i++, ++l_key_col_iter) {
        //std::cout << "inserted " << *l_key_col_iter << " " << i << endl;
        hash_table_.emplace(*l_key_col_iter, i);
    }
    // Probe phase

    auto r_key_col_iter = right_input_data[right_col_id].begin(0);
    int match_cnt = 0;
    for (uint64_t i = 0, limit = i + right_->result_size(); i != limit; i++, ++r_key_col_iter) {
        auto r_key = *r_key_col_iter;
        auto range = hash_table_.equal_range(r_key);
        //std::cout << r_key << endl;
        for (auto iter = range.first; iter != range.second; iter++) {
            match_cnt++;
            // 此处将左侧表的数据加入结果中，tempresults 的第一维应该是临时结果中的列号
            // 应该检查一下两种版本中的列号的变化情况。
            uint tmp_res_col_id = 0;
            for(unsigned col_idx = 0; col_idx < copyLeftData.size(); col_idx++) {
                tmp_results[tmp_res_col_id++].emplace_back(*(copyLeftData[col_idx].begin((*iter).second)));
            }
            for(auto& r_col_iter : r_col_iters) {
                tmp_results[tmp_res_col_id++].emplace_back(*r_col_iter);
            }
        }
        for(auto& iter : r_col_iters){
            ++iter;
        }
    }
    // std::cout << "left size is " << left_->result_size() << " hash table size is " << hash_table_.size() << " right size is " << right_->result_size() << " found " << match_cnt << " matches";
    for (int i = 0; i < col_size; i++) {
        results[i].addTuples(0, tmp_results[i].data(), tmp_results[i].size());
        results[i].fix();
    }
    __sync_fetch_and_add(&result_size_, tmp_results[0].size());
    finishAsyncRun(ioService,true);
    // UNIFINISHED multithread version
    // int cnt_task = THREAD_NUM;
    // task_length = right_->result_size() / cnt_task;
    // task_remainder = right_->result_size() % cnt_task;

    // if (task_length < minTuplesPerTask) {
    //     cnt_task = right_->resultSize / minTuplesPerTask;
    //     if (cnt_task == 0 ) cnt_task = 1;
    //     task_length = right_->resultSize/cnt_task;
    //     task_remainder = right_->resultSize%cnt_task;
    // }
    
    // pending_task = cnt_task;
    // for(int i = 0; i < cnt_task; i++) {
    //     tmp_results_.emplace_back();
    // }

    // uint64_t start = 0;
    // uint64_t remainder = task_remainder;
    // for(int i = 0; i < cnt_task; i++) {
    //     uint64_t length_left = task_length;
    //     if(remainder) {
    //         length_left++;
    //         remainder--;
    //     }
    //     ioService.post(bind(&Join::probingTask, this, &ioService, cnt_task, i, right_col_id, start, length_left));
    //     start += length_left;
    // }
}

void Join::copy2Result(uint64_t left_index, uint64_t right_index) {
    
}

// void Join::probingTask(boost::asio::io_service* ioService, int cntTask, int taskIndex, uint64_t right_col_id, uint64_t start, uint64_t length) {
//     auto right_key_col = right_input_data_[right_col_id];
//     auto right_col_it = right_key_col.begin(start);
//     auto& localResults = tmp_results_[taskIndex];
//     unsigned left_col_size = left_input_data_.size();
//     unsigned right_col_size = right_input_data_.size();
//     unsigned col_size = left_col_size + right_col_size;
//     vector<uint64_t*> copy_l_data, copy_r_data_;
//     __sync_synchronize();
//     if (is_stopped) {
//         goto probing_finish;
//     }

//     if (length == 0) {
//         goto probing_finish;
//     }
//     for (int j=0; j< col_size; j++) {
//         localResults.emplace_back();
//     }

//     for(auto& info : requested_columns_right_) {
//         copy_r_data_.push_back(local)
//     }
//     for (uint64_t i = start; i != start + length; i++, ++right_col_it) {
//         auto r_key = *right_col_it;
//         auto range = hash_table_.equal_range(r_key);
//         for(auto iter = range.first; iter != range.second; ++iter) {
//             for (unsigned col_id = 0; col_id < left_col_size; col_id++) {

//             }
//         }
//     }
//     //local result 为空的时候需要result和临时变量初始化
//     for (int i = 0; i < col_size; i++) {
//         results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
//     }
// 	__sync_fetch_and_add(&resultSize, localResults[0].size());

//     int remainder = __sync_sub_and_fetch(&pending_task, 1);
//     if (UNLIKELY(remainder == 0)) {
//         for (unsigned cId=0;cId<col_size;++cId) {
//             results[cId].fix();
//         }
//         finishAsyncRun(*ioService, true);
//         //input = nullptr;
//     }
// }

bool SelfJoin::require(SelectInfo info)
// Require a column and add it to results
{
    if (required_IUs_.count(info))
        return true;
    if(input_->require(info)) {
        required_IUs_.emplace(info);
        return true;
    }
    return false;
}
//---------------------------------------------------------------------------
void SelfJoin::asyncRun(boost::asio::io_service& ioService) {
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::asyncRun" << endl;
#endif
    pendingAsyncOperator = 1;
    input_->require(p_info_.left);
    input_->require(p_info_.right);
    __sync_synchronize();
    input_->asyncRun(ioService);
}
//---------------------------------------------------------------------------
void SelfJoin::selfJoinTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    auto& inputData=input_->getResults();
    vector<vector<uint64_t>>& localResults = tmpResults[taskIndex];
    auto leftColId=input_->resolve(p_info_.left);
    auto rightColId=input_->resolve(p_info_.right);

    auto leftColIt=inputData[leftColId].begin(start);
    auto rightColIt=inputData[rightColId].begin(start);

    vector<Column<uint64_t>::Iterator> colIt;

    unsigned colSize = copy_data_.size();
    
    for (int j=0; j<colSize; j++) {
        localResults.emplace_back();
    }
    
    for (unsigned i=0; i<colSize; i++) {
        colIt.push_back(copy_data_[i]->begin(start));
    }
    for (uint64_t i=start, limit=start+length;i<limit;++i) {
        if (*leftColIt==*rightColIt) {
            for (unsigned cId=0;cId<colSize;++cId) {
                localResults[cId].push_back(*(colIt[cId]));
            }
        }
        ++leftColIt;
        ++rightColIt;
        for (unsigned i=0; i<colSize; i++) {
            ++colIt[i];
        }
    }
    //local result 为空的时候需要result和临时变量初始化
    for (int i=0; i<colSize; i++) {
        results[i].addTuples(taskIndex, localResults[i].data(), localResults[i].size());
    }
	__sync_fetch_and_add(&result_size_, localResults[0].size());

    int remainder = __sync_sub_and_fetch(&pendingTask, 1);
    if (UNLIKELY(remainder == 0)) {
        for (unsigned cId=0;cId<colSize;++cId) {
            results[cId].fix();
        }
        finishAsyncRun(*ioService, true);
        //input = nullptr;
    }
}
//---------------------------------------------------------------------------
void SelfJoin::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "SelfJoin("<< queryIndex << "," << operatorIndex <<")::createAsyncTasks" << endl;
#endif
    uint64_t result_size = input_->result_size();
    if (input_->result_size() == 0) {
        finishAsyncRun(ioService, true);
        return;
    }
    
    int cntTask = THREAD_NUM;
    uint64_t taskLength = result_size / cntTask;
    uint64_t rest = result_size %cntTask;
    
    if (taskLength < minTuplesPerTask) {
        cntTask = result_size/minTuplesPerTask;
        if (cntTask == 0)
            cntTask = 1;
        taskLength = result_size/cntTask;
        rest = result_size %cntTask;
    }
    
    auto& inputData=input_->getResults();
    
    for (auto& iu : required_IUs_) {
        auto id=input_->resolve(iu);
        copy_data_.emplace_back(&inputData[id]);
        select_to_result_col_id_.emplace(iu,copy_data_.size()-1);
		results.emplace_back(cntTask);
    }

    for (int i=0; i<cntTask; i++) {
        tmpResults.emplace_back();
    } 
    
    pendingTask = cntTask; 
    __sync_synchronize();
    uint64_t start = 0;
    for (int i=0; i<cntTask; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&SelfJoin::selfJoinTask, this, &ioService, i, start, length)); 
        start += length;
    }
}

void Checksum::asyncRun(boost::asio::io_service& ioService, int queryIndex) {
    #ifdef VERBOSE
        cout << "Checksum(" << queryIndex << "," << operatorIndex << ")::asyncRun()" << endl;
    #endif
        this->query_index = queryIndex;
        pendingAsyncOperator = 1;
        for (auto& sInfo : col_info_) {
            input_->require(sInfo);
        }
        __sync_synchronize();
        input_->asyncRun(ioService);
    //    cout << "Checksum::asyncRun" << endl;
}

void Checksum::createAsyncTasks(boost::asio::io_service& ioService) {
    assert (pendingAsyncOperator==0);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ")::createAsyncTasks" << endl;
#endif
    for (auto& sInfo : col_info_) {
        check_sums_.push_back(0);
    }
    
    if (input_->result_size() == 0) {
        finishAsyncRun(ioService, false);
        return;
    }
    
    int cnt_task = THREAD_NUM;
    uint64_t taskLength = input_->result_size()/cnt_task;
    uint64_t rest = input_->result_size() % cnt_task;
    
    if (taskLength < min_tuples_per_task) {
        cnt_task = input_->result_size() / min_tuples_per_task;
        if (cnt_task == 0)
        cnt_task = 1;
        taskLength = input_->result_size() / cnt_task;
        rest = input_->result_size() % cnt_task;
    }
#ifdef VERBOSE 
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") input size: " << input_->result_size() << " cntTask: " << cnt_task << " length: " << taskLength << " rest: " << rest <<  endl;
#endif
    pending_task = cnt_task; 
    __sync_synchronize();
    uint64_t start = 0;
    for (int i=0; i<cnt_task; i++) {
        uint64_t length = taskLength;
        if (rest) {
            length++;
            rest--;
        }
        ioService.post(bind(&Checksum::checksumTask, this, &ioService, i, start, length)); 
        start += length;
    }
}

void Checksum::checksumTask(boost::asio::io_service* ioService, int taskIndex, uint64_t start, uint64_t length) {
    auto& input_data = input_->getResults();
     
    int sumIndex = 0;
    for (auto& sInfo : col_info_) {
        auto colId = input_->resolve(sInfo);
        auto input_col_it = input_data[colId].begin(start);
        uint64_t sum=0;
        for (int i=0; i<length; i++,++input_col_it){
            // if(i > 487) {
            // cout << i << endl;
            // }
            sum += (*input_col_it);
        }
        __sync_fetch_and_add(&check_sums_[sumIndex++], sum);
    }
    
    int remainder = __sync_sub_and_fetch(&pending_task, 1);
    if (UNLIKELY(remainder == 0)) {
        finishAsyncRun(*ioService, false);
        //input = nullptr;
    }
}

void Checksum::finishAsyncRun(boost::asio::io_service& ioService, bool startParentAsync) {
    joiner.asyncResults[query_index] = std::move(check_sums_);
    int pending = __sync_sub_and_fetch(&joiner.pendingAsyncJoin, 1);
#ifdef VERBOSE
    cout << "Checksum(" << queryIndex << "," << operatorIndex <<  ") finish query index: " << queryIndex << " rest quries: "<< pending << endl;
#endif
    assert(pending >= 0);
    if (pending == 0) {
#ifdef VERBOSE
        cout << "A query set is done. " << endl;
#endif
        unique_lock<mutex> lk(joiner.cvAsyncMutex); // guard for missing notification
        joiner.cvAsync.notify_one();
    }
}

void Checksum::printAsyncInfo() {
	cout << "pendingChecksum : " << pending_task << endl;
	input_->printAsyncInfo();
}

void SelfJoin::printAsyncInfo() {
	cout << "pendingSelfJoin : " << pendingTask << endl;
	input_->printAsyncInfo();
}
void FilterScan::printAsyncInfo() {
	cout << "pendingFilterScan : " << pendingTask << endl;
}
void Scan::printAsyncInfo() {
}

uint64_t Scan::getResultsSize() {
    return results.size()* relation_.size()*8; 
}

void Join::printAsyncInfo() {
	left_->printAsyncInfo();
	right_->printAsyncInfo();
}