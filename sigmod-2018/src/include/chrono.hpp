#include <chrono>
#include <unordered_map>
#include <string>
#include <mutex>
#include <iostream>
#include <fstream>

class BarrierProfiler {
public:
    // 获取单例实例
    static BarrierProfiler& getInstance() {
        static BarrierProfiler instance;
        return instance;
    }

    // 记录屏障耗时
    void record(const std::string& barrierName, std::chrono::nanoseconds duration) {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_[barrierName].totalTime += duration;
        stats_[barrierName].count++;
    }

    void record_start(const std::string& op_name, unsigned op_idx, std::chrono::_V2::system_clock::time_point start) {
        if(start_time_map.count(op_name) == 0) {
            start_time_map[op_name];
        }
        
        if(start_time_map[op_name].count(op_idx) != 0) {
            double_start.emplace(op_name);
        } else {
            start_time_map[op_name][op_idx] = start;
        }
    }

    void record_finish(const std::string& op_name, unsigned op_idx, std::chrono::_V2::system_clock::time_point end) {
        if(start_time_map[op_name].count(op_idx) == 0) {
            end_with_no_start.emplace(op_name);
            return;
        }
        time_cost_map[op_name] += end - start_time_map[op_name][op_idx];
        start_time_map[op_name].erase(op_idx);
    }

    void setOutputFile(const std::string& filePath) {
        std::lock_guard<std::mutex> lock(mutex_);
        outputFile_.open(filePath);
        
        if (!outputFile_.is_open()) {
            throw std::runtime_error("Failed to open output file: " + filePath);
        }
        std::ostream& out = outputFile_;
        out << "opened" << "\n";
    }

    // 打印统计结果
    // void printStats() const {
    //     std::lock_guard<std::mutex> lock(mutex_);
    //     std::ostream& out = outputFile_.is_open() ? outputFile_ : std::cout;
    //     out << "writing" << "\n";
    //     out << "data size is " << stats_.size() << "\n";
    //     for (const auto& [name, data] : stats_) {
    //         out << "Barrier '" << name << "': "
    //             << "count=" << data.count << ", "
    //             << "total_time=" << std::chrono::duration_cast<std::chrono::milliseconds>(data.totalTime).count() << " ms, "
    //             << "avg_time=" << (data.count > 0 ? data.totalTime.count() / data.count : 0) << " ns\n";
    //     }

    //     if (outputFile_.is_open()) {
    //         outputFile_.flush(); // 确保数据写入文件
    //     }
    // }

    void printStats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostream& out = outputFile_.is_open() ? outputFile_ : std::cout;
        out << "writing" << "\n";
        out << "data size is " << time_cost_map.size() << "\n";
        for (const auto& [name, data] : time_cost_map) {
            out << "Operator '" << name << "': "
                << "total_time=" << std::chrono::duration_cast<std::chrono::milliseconds>(data).count() << " ms, \n";
                // << "avg_time=" << (data.count > 0 ? data.totalTime.count() / data.count : 0) << " ns\n";
        }
        out << "double_start : ";
        for(auto& name : double_start) {
            out << name << "\n";
        }
        out << "end with no start : ";
        for(auto& name : end_with_no_start) {
            out << name << "\n";
        }

        if (outputFile_.is_open()) {
            outputFile_.flush(); // 确保数据写入文件
        }
    }

private:
    // 禁用外部构造/拷贝
    BarrierProfiler() = default;
    BarrierProfiler(const BarrierProfiler&) = delete;
    BarrierProfiler& operator=(const BarrierProfiler&) = delete;

    struct BarrierStats {
        std::chrono::nanoseconds totalTime{0};
        size_t count{0};
    };

    std::unordered_map<std::string, std::unordered_map<int, std::chrono::_V2::system_clock::time_point>> start_time_map;

    std::unordered_map<std::string, std::chrono::nanoseconds> time_cost_map;

    std::unordered_set<std::string> double_start, end_with_no_start;

    std::unordered_map<std::string, BarrierStats> stats_;
    mutable std::mutex mutex_;  // 保证线程安全

    mutable std::ofstream outputFile_; // 文件输出流
};