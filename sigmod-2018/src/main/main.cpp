#include <iostream>
#include <filesystem>

#include "joiner.hpp"
#include "parser.hpp"

using namespace std;

int main(int argc, char *argv[]) {
    Joiner joiner(THREAD_NUM);
    namespace fs = std::filesystem;

    fs::path current_dir = fs::current_path();
    fs::path parent_dir = current_dir.parent_path();
    fs::current_path(parent_dir);

    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    // Preparation phase (not timed)
    // Build histograms, indexes,...

    QueryInfo i;
    while (getline(std::cin, line)) {
        if (line == "F") { // End of a batch
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto& result : results)
                cout << result;
            continue;
        } 
        joiner.createAsyncQueryTask(line);
    }

    return 0;
}
